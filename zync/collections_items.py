from typing import Optional, Dict, List
from dataclasses import dataclass, field

import numpy as np

from .utils import query_from_db, clean_string


@dataclass
class Collection:
    collection_id: int
    name: str
    key: str  # TODO is this used?
    parent_id: Optional[int] = None
    full_path: str = ""
    item_ids: List[int] = field(default_factory=list)

    def __post_init__(self):
        self.collection_id = int(self.collection_id)
        if np.isnan(self.parent_id):
            self.parent_id = None
        else:
            self.parent_id = int(self.parent_id)
        self.name = self.name.strip().replace(" ", "_")

        self._update_item_ids()

    def _update_item_ids(self):
        qry = f"""
        select ia.itemID  -- use child item ids
        from collectionItems i
        join itemAttachments ia on i.itemID = ia.parentItemID
        where collectionID = {self.collection_id}
        """
        df = query_from_db(qry, ["item_id"])
        self.item_ids = df.item_id.astype(int).tolist()

    def update_full_path(self, all_collections: Dict):
        # TODO or use path object?
        reverse_list = [self.name]
        parent_id = self.parent_id
        while parent_id:
            parent = all_collections[parent_id]
            reverse_list.append(parent.name)
            parent_id = parent.parent_id
        self.full_path = "/".join(reverse_list[::-1])


@dataclass
class Item:
    item_id: int
    parent_item_id: int
    item_type_id: int  # TODO is this useful?
    key: str
    author_last_name: str
    author_first_name: str
    zotero_path: str
    title: str = ""
    publish_year: str = "yyyy"
    extra_tag: Optional[str] = None

    def __post_init__(self):
        self.item_id = int(self.item_id)
        self.parent_item_id = int(self.parent_item_id)
        self.item_type_id = int(self.item_type_id)
        self.author_last_name = clean_string(self.author_last_name, replace_space=True)
        self.author_first_name = clean_string(
            self.author_first_name, replace_space=True
        )

        self._update_zotero_path()
        self._update_title()
        self._update_publish_year()
        self._update_extra_tag()

    def _qry_field(self, field_name: str):
        qry = f"""
        select idv.value
        from items i
        join itemTypeFields itf on itf.itemTypeID = i.itemTypeID
        join fields f on itf.fieldID = f.fieldID
        join itemData id on id.fieldID = f.fieldID and id.itemID = i.itemid
        join itemDataValues idv on idv.valueID = id.valueID
        where i.itemID = {self.parent_item_id}
        and f.fieldName = '{field_name}'
        """
        return qry

    def _update_zotero_path(self):
        self.zotero_path = self.zotero_path.split("storage:")[-1]

    def _update_title(self):
        qry = self._qry_field("title")
        df = query_from_db(qry, ["title"])
        self.title = clean_string(df.title.iloc[0].title(), replace_space=True)

    def _update_publish_year(self):
        qry = self._qry_field("date")
        df = query_from_db(qry, ["date"])
        if not df.empty:
            self.publish_year = str(df.date.iloc[0])[:4]

    def _update_extra_tag(self):
        qry = self._qry_field("extra")
        df = query_from_db(qry, ["extra"])
        if df.empty:
            return
        if df.extra.iloc[0] == "TOREAD":  # TODO allow other tags here
            self.extra_tag = "TOREAD"


def get_collections() -> Dict[int, Collection]:
    qry_collections = """
    select collectionID, collectionName, parentCollectionId, key
    from collections
    """
    df_collections = query_from_db(
        qry_collections, ["collection_id", "collection_name", "parent_id", "key"]
    )

    all_collections = dict()

    for ct, row in df_collections.iterrows():
        collection = Collection(
            collection_id=row.collection_id,
            name=row.collection_name,
            key=row.key,
            parent_id=row.parent_id,
        )
        all_collections[row.collection_id] = collection

    for _, c in all_collections.items():
        c.update_full_path(all_collections)
    return all_collections


def get_items() -> Dict[int, Item]:
    qry_items = """
    select ia.itemID, i.itemID as parent_item_id, i.itemTypeID, i2.key, lastName, firstName, ia.path
    from items i -- parent item
    join itemAttachments ia on i.itemID = ia.parentItemID
    join items i2 on ia.itemID = i2.itemID  -- to select the keys of the child item
    join itemCreators ic using (itemId)
    join creators c using (creatorID)
    where ic.orderIndex = 0  -- only take name of first author
    and ia.contentType = 'application/pdf'
    """
    df_items = query_from_db(
        qry_items,
        [
            "item_id",
            "parent_item_id",
            "item_type_id",
            "key",
            "author_last_name",
            "author_first_name",
            "path",
        ],
    )

    all_items = dict()

    for ct, row in df_items.iterrows():
        item = Item(
            item_id=row.item_id,
            parent_item_id=row.parent_item_id,
            item_type_id=row.item_type_id,
            key=row.key,
            author_last_name=row.author_last_name,
            author_first_name=row.author_first_name,
            zotero_path=row.path,
        )
        all_items[row.item_id] = item

    return all_items
