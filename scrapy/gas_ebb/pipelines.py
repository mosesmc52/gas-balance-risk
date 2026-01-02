from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict

import pymongo
from pymongo.errors import DuplicateKeyError


class MongoPipeline:
    """
    Inserts Scrapy items into MongoDB.
    Supports optional upsert by a unique key, and adds metadata timestamps.
    """

    def __init__(
        self,
        mongo_uri: str,
        mongo_db: str,
        mongo_collection: str,
        upsert_key: str | None,
    ):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        self.upsert_key = upsert_key

        self.client: pymongo.MongoClient | None = None
        self.collection: pymongo.collection.Collection | None = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
            mongo_db=crawler.settings.get("MONGO_DATABASE", "scrapy"),
            mongo_collection=crawler.settings.get("MONGO_COLLECTION", "items"),
            upsert_key=crawler.settings.get("MONGO_UPSERT_KEY"),  # optional
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        db = self.client[self.mongo_db]
        self.collection = db[self.mongo_collection]

        # Optional: create an index if using upsert
        if self.upsert_key:
            self.collection.create_index([(self.upsert_key, 1)], unique=True)

        spider.logger.info(
            "Connected to MongoDB: db=%s collection=%s upsert_key=%s",
            self.mongo_db,
            self.mongo_collection,
            self.upsert_key,
        )

    def close_spider(self, spider):
        if self.client:
            self.client.close()

    def process_item(self, item, spider):
        if not self.collection:
            return item

        doc: Dict[str, Any] = dict(item)

        # Add basic metadata
        now = datetime.now(timezone.utc)
        doc.setdefault("_meta", {})
        doc["_meta"]["scraped_at_utc"] = now.isoformat()

        if self.upsert_key and self.upsert_key in doc:
            key_val = doc[self.upsert_key]
            self.collection.update_one(
                {self.upsert_key: key_val},
                {
                    "$set": doc,
                    "$setOnInsert": {"_meta.created_at_utc": now.isoformat()},
                },
                upsert=True,
            )
        else:
            try:
                self.collection.insert_one(doc)
            except DuplicateKeyError:
                # In case _id or other unique indexes collide
                spider.logger.warning(
                    "Duplicate key, skipping insert: %s", doc.get("_id")
                )

        return item
