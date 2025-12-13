import time
import argparse
import logging

from helper import chance, clamp_probs
from operations import *
from config import env_db_config, connect

def parse_args():
    parser = argparse.ArgumentParser(
        description="Simulate ongoing writes/updates/deletes in Postgres for CDC."
    )

    parser.add_argument("--sleep-ms", type=int, default=500)
    parser.add_argument("--insert-p", type=float, default=0.60)
    parser.add_argument("--update-p", type=float, default=0.35)
    parser.add_argument("--delete-p", type=float, default=0.05)

    parser.add_argument("--customer-insert-ratio", type=float, default=0.25)
    parser.add_argument("--customer-update-ratio", type=float, default=0.10)
    parser.add_argument("--customer-delete-ratio", type=float, default=0.05)

    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )

    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single operation and exit (useful for testing).",
    )

    return parser.parse_args()


def setup_logging(level: str) -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    return logging.getLogger("simulate_writes")


def choose_operation(insert_p: float, update_p: float) -> str:
    r = random.random()
    if r < insert_p:
        return "insert"
    if r < insert_p + update_p:
        return "update"
    return "delete"


def handle_insert(cur, args, log):
    if chance(args.customer_insert_ratio):
        cid = create_customer(cur)
        oid = create_order(cur, cid)
        log.info("[INSERT] customer_id=%s, order_id=%s", cid, oid)
        return

    cid = get_random_customer_id(cur)
    if cid is None:
        cid = create_customer(cur)

    oid = create_order(cur, cid)
    log.info("[INSERT] order_id=%s (customer_id=%s)", oid, cid)


def handle_update(cur, args, log):
    if chance(args.customer_update_ratio):
        cid = update_random_customer(cur)
        if cid is None:
            log.info("[UPDATE] skipped (no customers)")
            return
        log.info("[UPDATE] customer_id=%s", cid)
        return

    oid = update_random_order(cur)
    if oid is None:
        log.info("[UPDATE] skipped (no orders)")
        return
    log.info("[UPDATE] order_id=%s", oid)


def handle_delete(cur, args, log):
    if chance(args.customer_delete_ratio):
        try:
            cid = delete_random_customer(cur)
            if cid is None:
                log.info("[DELETE] skipped (no customers)")
                return
            log.info("[DELETE] customer_id=%s", cid)
        except Exception as e:
            log.warning(
                "[DELETE] customer failed (likely FK constraint): %s",
                str(e),
            )
        return

    oid = delete_random_order(cur)
    if oid is None:
        log.info("[DELETE] skipped (no orders)")
        return
    log.info("[DELETE] order_id=%s", oid)


def run_once(conn, args, log, insert_p, update_p):
    with conn.cursor() as cur:
        op = choose_operation(insert_p, update_p)

        if op == "insert":
            handle_insert(cur, args, log)
        elif op == "update":
            handle_update(cur, args, log)
        else:
            handle_delete(cur, args, log)

    conn.commit()


def main():
    args = parse_args()
    log = setup_logging(args.log_level)

    insert_p, update_p, delete_p = clamp_probs(
        args.insert_p, args.update_p, args.delete_p
    )

    log.info(
        "Starting simulator with normalized probs: insert=%.2f update=%.2f delete=%.2f",
        insert_p,
        update_p,
        delete_p,
    )

    db = env_db_config()
    log.info(
        "Connecting to Postgres %s:%d/%s as %s",
        db.host,
        db.port,
        db.dbname,
        db.user,
    )

    while True:
        try:
            with connect(db) as conn:
                conn.autocommit = False
                run_once(conn, args, log, insert_p, update_p)

            if args.once:
                log.info("Ran once; exiting.")
                return

            time.sleep(max(args.sleep_ms, 0) / 1000.0)

        except KeyboardInterrupt:
            log.info("Interrupted; exiting.")
            return

        except Exception as e:
            log.exception("Simulator error: %s", str(e))
            time.sleep(1.0)


if __name__ == "__main__":
    main()

