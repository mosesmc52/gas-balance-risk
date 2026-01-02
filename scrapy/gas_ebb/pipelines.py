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

        # Per-spider override
        collection_name = getattr(
            spider,
            "mongo_collection",
            self.mongo_collection,
        )

        self.collection = db[collection_name]

        unique_fields = getattr(spider, "mongo_unique_fields", None)
        if unique_fields:
            index_spec = [(f, 1) for f in unique_fields]
            self.collection.create_index(index_spec, unique=True)

        spider.logger.info(
            "Mongo collection set to '%s' for spider '%s'",
            collection_name,
            spider.name,
        )

    def close_spider(self, spider):
        if self.client:
            self.client.close()

    def process_item(self, item, spider):
        if self.collection is None:
            return item

        doc = dict(item)
        now = datetime.now(timezone.utc).isoformat()

        unique_fields = getattr(spider, "mongo_unique_fields", None)

        if unique_fields:
            missing = [f for f in unique_fields if f not in doc or doc[f] in (None, "")]
            if missing:
                spider.logger.warning(
                    "Missing unique fields %s; inserting without dedupe", missing
                )
                self.collection.insert_one(doc)
                return item

            filt = {f: doc[f] for f in unique_fields}
            self.collection.update_one(
                filt,
                {
                    "$set": doc,
                    "$setOnInsert": {"_meta.created_at_utc": now},
                    "$currentDate": {"_meta.updated_at": True},
                },
                upsert=True,
            )
        else:
            self.collection.insert_one(doc)

        return item
