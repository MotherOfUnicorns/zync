from typing import Optional, Dict, List
from dataclasses import dataclass, field

import numpy as np

from .utils import query_from_db


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
        select itemID from collectionItems
        where collectionID = {self.collection_id}
        """
        df = query_from_db(qry, ['item_id'])
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
