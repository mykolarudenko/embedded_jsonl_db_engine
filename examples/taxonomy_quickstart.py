#!/usr/bin/env python3
# Example: working with taxonomies (list membership and single reference)
# - Defines a schema with two taxonomy fields:
#   * tags: list[str] with membership index and non-strict validation
#   * category: single string reference with strict validation
# - Demonstrates taxonomy upsert, rename, merge, delete (detach)
# - Inserts sample records and runs queries before/after taxonomy changes

import os
from embedded_jsonl_db_engine import Database

SCHEMA = {
    "id": {"type": "str", "mandatory": False, "index": True},
    "name": {"type": "str", "mandatory": True, "index": True},
    "category": {"type": "str", "mandatory": False, "taxonomy": "categories", "taxonomy_mode": "single", "strict": True, "index": True},
    "tags": {"type": "list", "mandatory": False, "index_membership": True, "taxonomy": "tags", "strict": False},
    "createdAt": {"type": "datetime", "mandatory": False},
}

def ensure_dir(p: str) -> None:
    d = os.path.dirname(p)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def main() -> None:
    base_dir = os.path.join(os.path.dirname(__file__), "data")
    ensure_dir(base_dir)
    path = os.path.join(base_dir, "taxonomy.jsonl")

    # Fresh DB
    db = Database(path, schema=SCHEMA, mode="+")

    # Define taxonomy entries
    cats = db.taxonomy("categories")
    cats.upsert("news", title="News")
    cats.upsert("tech", title="Technology")
    cats.upsert("life", title="Lifestyle")

    tags = db.taxonomy("tags")
    tags.upsert("red", title="Red")
    tags.upsert("blue", title="Blue")
    tags.upsert("navy", title="Navy")
    tags.upsert("old", title="Deprecated")

    # Insert sample records
    r1 = db.new(); r1["name"] = "Post A"; r1["category"] = "news"; r1["tags"] = ["red", "blue"]; r1.save()
    r2 = db.new(); r2["name"] = "Post B"; r2["category"] = "tech"; r2["tags"] = ["navy"]; r2.save()
    r3 = db.new(); r3["name"] = "Post C"; r3["category"] = "life"; r3["tags"] = ["old"]; r3.save()

    print("Initial stats (by category):", db.taxonomy("categories").stats())
    print("Initial stats (by tags):", db.taxonomy("tags").stats())

    # Rename a tag key ("red" -> "scarlet") across all records (full-file rewrite)
    db.taxonomy("tags").rename("red", "scarlet", collision="merge")
    print("After rename('red'->'scarlet') tags stats:", db.taxonomy("tags").stats())

    # Merge two tags into one ("blue","navy" -> "blue")
    db.taxonomy("tags").merge(["blue", "navy"], "blue")
    print("After merge(['blue','navy']->'blue') tags stats:", db.taxonomy("tags").stats())

    # Delete a tag and detach it from records
    db.taxonomy("tags").delete("old", strategy="detach")
    print("After delete('old', detach) tags stats:", db.taxonomy("tags").stats())

    # Query by taxonomy:
    # - list membership ($contains) works with tags
    print("Records with tag 'scarlet':",
          [rec["name"] for rec in db.find({"tags": {"$contains": "scarlet"}})])
    # - equality on single taxonomy reference
    print("Records in category 'tech':",
          [rec["name"] for rec in db.find({"category": {"$eq": "tech"}})])

    db.close()
    print("Done.")

if __name__ == "__main__":
    main()
