from app.models.catalog import Catalog

class Cat_ProductBrand(Catalog):
    table_name = "cat_products_brands"

    @classmethod
    def from_row(cls, row):
        # return cls(
        #     _id=row["_id"],
        #     _name=row["_name"],
        #     _description=row["_description"],
        #     _price=row["_price"],
        #     _created_at=row["_created_at"],
        #     _created_by=row["_created_by"]
        # )

        # реалізуйте ініціалізацію об'єкта Product з dict
        ...