"""
Скрипт для повторного парсинга последних N писем по каждому отправителю.

Что делает:
  1. Читает из settlement_emails последние EMAILS_PER_SENDER писем
     по каждому известному отправителю (status != 'SKIPPED').
  2. Для сендеров с PDF/Excel-парсером — пропускает письма без вложений
     (attachment_count = 0), т.к. без файла парсить нечего.
     Для сендеров с email-body парсером (STONEX_REPO_EMAIL, GRANT_WESTOVER_REPO_EMAIL,
     FAB_REPO_EMAIL) — берёт любые письма.
  3. Удаляет их из БД (reconciliation → trades → files → emails).
  4. Перезапускает process_message() — письма снова парсятся и сохраняются.

Переменные окружения (те же, что у основного function_app.py):
  PG_CONN_STRING, TENANT_ID, CLIENT_ID, CLIENT_SECRET
  GRAPH_MAILBOX  (по умолчанию back.office@amwealth.ae)

Запуск:
  python reparse_recent.py
  python reparse_recent.py --dry-run      # только покажет что будет сделано, без изменений
  python reparse_recent.py --n 1          # по 1 письму на отправителя
"""

import argparse
import logging
import sys
from function_app import (
    get_conn,
    get_graph_token,
    load_mapping,
    detect_template_from_mapping,
    process_message,
    start_agent_run,
    finish_agent_run,
    get_message_full,
    GRAPH_MAILBOX,
    EMAIL_BODY_TEMPLATES,
    normalize_email_address,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


def get_recent_emails_per_sender(conn, n: int) -> list[dict]:
    """
    Возвращает последние n писем на каждого отправителя (status != 'SKIPPED').
    Поля: internet_message_id, message_id, sender, subject, received_at, status, attachment_count
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                internet_message_id,
                message_id,
                sender,
                subject,
                received_at,
                status,
                attachment_count
            from (
                select *,
                    row_number() over (
                        partition by sender
                        order by received_at desc nulls last
                    ) as rn
                from back_office_auto.settlement_emails
                where status <> 'SKIPPED'
            ) ranked
            where rn <= %s
            order by sender, received_at desc
            """,
            (n,),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def delete_email_from_db(conn, internet_message_id: str):
    """Удаляет все записи письма из всех таблиц (каскад вручную)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            delete from back_office_auto.settlement_reconciliation
            where settlement_trade_id in (
                select id from back_office_auto.settlement_trades
                where internet_message_id = %s
            )
            """,
            (internet_message_id,),
        )
        cur.execute(
            "delete from back_office_auto.settlement_trades where internet_message_id = %s",
            (internet_message_id,),
        )
        cur.execute(
            "delete from back_office_auto.settlement_files where internet_message_id = %s",
            (internet_message_id,),
        )
        cur.execute(
            "delete from back_office_auto.settlement_emails where internet_message_id = %s",
            (internet_message_id,),
        )
    conn.commit()


def fetch_graph_message_stub(token: str, mailbox: str, message_id: str) -> dict:
    """
    Загружает заголовки письма из Graph API (id, internetMessageId, subject,
    receivedDateTime, from, hasAttachments).
    """
    import requests

    url = (
        f"https://graph.microsoft.com/v1.0/users/{mailbox}/messages/{message_id}"
        f"?$select=id,internetMessageId,subject,receivedDateTime,from,hasAttachments"
    )
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=60)
    r.raise_for_status()
    return r.json()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=2, help="Писем на отправителя (default 2)")
    parser.add_argument("--dry-run", action="store_true", help="Только показать, не менять БД")
    args = parser.parse_args()

    log.info("=== reparse_recent.py  n=%d  dry_run=%s ===", args.n, args.dry_run)

    conn = get_conn()
    token = get_graph_token()
    mapping_by_sender = load_mapping(conn)

    emails = get_recent_emails_per_sender(conn, args.n)

    if not emails:
        log.warning("Нет писем для повторного парсинга (settlement_emails пуст или только SKIPPED).")
        conn.close()
        return

    # Filter: for non-body-parsers skip emails without attachments
    filtered = []
    skipped_no_attach = []
    for e in emails:
        template = detect_template_from_mapping(e["sender"], mapping_by_sender)
        if template not in EMAIL_BODY_TEMPLATES and (e.get("attachment_count") or 0) == 0:
            skipped_no_attach.append(e)
        else:
            filtered.append(e)

    if skipped_no_attach:
        log.info(
            "Пропущено %d писем без вложений (PDF/Excel парсеры требуют файл):",
            len(skipped_no_attach),
        )
        for e in skipped_no_attach:
            log.info(
                "  SKIP  %-40s  %s  [%s]",
                e["sender"],
                e["received_at"],
                e["internet_message_id"][:40],
            )

    if not filtered:
        log.warning("После фильтрации нет писем для обработки.")
        conn.close()
        return

    log.info("Найдено %d писем для повторного парсинга:", len(filtered))
    for e in filtered:
        template = detect_template_from_mapping(e["sender"], mapping_by_sender)
        log.info(
            "  %-40s  %-30s  %-12s  attach=%s  %s",
            e["sender"],
            template or "?",
            e["status"],
            e.get("attachment_count"),
            e["received_at"],
        )

    if args.dry_run:
        log.info("DRY RUN — изменений нет.")
        conn.close()
        return

    run_id = start_agent_run(conn, "reparse_recent")

    results = {"ok": 0, "failed": 0, "skipped": 0}

    for e in filtered:
        imid = e["internet_message_id"]
        message_id = e["message_id"]
        sender = e["sender"]

        log.info("--- [%s] %s ---", sender, imid[:50])

        # 1. Удалить из БД
        try:
            delete_email_from_db(conn, imid)
            log.info("    БД очищена")
        except Exception as ex:
            log.error("    Ошибка при удалении из БД: %s", ex)
            conn.rollback()
            results["failed"] += 1
            continue

        # 2. Загрузить заголовки письма из Graph API
        try:
            msg_stub = fetch_graph_message_stub(token, GRAPH_MAILBOX, message_id)
        except Exception as ex:
            log.error("    Не удалось получить письмо из Graph API: %s", ex)
            results["failed"] += 1
            continue

        # 3. Запустить парсинг
        try:
            status, count = process_message(
                conn=conn,
                token=token,
                mailbox=GRAPH_MAILBOX,
                msg=msg_stub,
                mapping_by_sender=mapping_by_sender,
                processing_run_id=run_id,
            )
            log.info("    Результат: status=%s  trades=%d", status, count)
            if status in ("PARSED", "NO_TRADES_FOUND"):
                results["ok"] += 1
            else:
                results["skipped"] += 1
        except Exception as ex:
            log.error("    Ошибка при парсинге: %s", ex)
            conn.rollback()
            results["failed"] += 1

    finish_agent_run(
        conn,
        run_id,
        "SUCCESS" if results["failed"] == 0 else "PARTIAL",
        f"ok={results['ok']} skipped={results['skipped']} failed={results['failed']}",
    )

    conn.close()
    log.info("=== Готово: ok=%d  skipped=%d  failed=%d ===", results["ok"], results["skipped"], results["failed"])


if __name__ == "__main__":
    main()
