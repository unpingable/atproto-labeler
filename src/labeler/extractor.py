from typing import List, Tuple, Dict, Any
from . import timeutil


def extract_edges_from_event(event: Dict[str, Any]) -> List[Tuple[str, str, str, str]]:
    """Return list of edges as tuples (src_did, dst_did, type, ctime_iso).

    The function is conservative: it looks for common reply/repost patterns in
    `event` and returns zero or more edges to insert into the `edges` table.
    """
    edges = []
    ctime = event.get("time") or event.get("record", {}).get("indexedAt")
    if isinstance(ctime, (int, float)):
        ctime = timeutil.to_utc_iso(ctime)
    if not ctime:
        ctime = timeutil.now_utc().isoformat()
    else:
        ctime = timeutil.to_utc_iso(ctime)

    author = event.get("author") or event.get("at") or event.get("did")

    # Example: reply chain
    record = event.get("record", {})
    reply = record.get("reply") if isinstance(record, dict) else None
    if reply:
        parent = reply.get("parent")
        if parent:
            parent_author = parent.get("author")
            if parent_author and author:
                edges.append((author, parent_author, "reply", ctime))

    # Example: explicit repost event types
    if event.get("type") and "repost" in event.get("type"):
        # assume record has 'subject' with 'author'
        subject = record.get("subject") if isinstance(record, dict) else None
        if subject and isinstance(subject, dict):
            subj_author = subject.get("author")
            if subj_author and author:
                edges.append((author, subj_author, "repost", ctime))

    # Example: quote-post embed
    embed = record.get("embed") if isinstance(record, dict) else None
    if embed and isinstance(embed, dict):
        orig = embed.get("record") or {}
        if isinstance(orig, dict):
            orig_author = orig.get("author")
            if orig_author and author:
                edges.append((author, orig_author, "quote", ctime))

    return edges
