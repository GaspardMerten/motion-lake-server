#  Copyright (c) 2024. Gaspard Merten
#  All rights reserved.

from sqlalchemy import ForeignKey, DateTime
from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column


class Base(DeclarativeBase):
    pass


class Collection(Base):
    __tablename__ = "collection"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(256), unique=True)

    def __repr__(self) -> str:
        return f"Collection(id={self.id!r}, name={self.name!r})"


class Fragment(Base):
    __tablename__ = "fragment"

    uuid: Mapped[str] = mapped_column(primary_key=True)
    content_type: Mapped[int] = mapped_column(nullable=True)
    collection_id: Mapped[int] = mapped_column(ForeignKey("collection.id"))

    def __repr__(self) -> str:
        return f"Fragment(uuid={self.uuid!r})"


class BufferedFragment(Base):
    __tablename__ = "buffered_fragment"

    timestamp: Mapped[str] = mapped_column(DateTime, primary_key=True)
    collection_id: Mapped[int] = mapped_column(
        ForeignKey("collection.id"), primary_key=True
    )
    content_type: Mapped[int] = mapped_column()
    size: Mapped[int] = mapped_column()
    original_size: Mapped[int] = mapped_column()
    uuid: Mapped[str] = mapped_column()
    locked: Mapped[bool] = mapped_column(default=False)
    hash: Mapped[str] = mapped_column()

    def __repr__(self) -> str:
        return f"BufferedFragment(uuid={self.uuid!r})"


class Item(Base):
    __tablename__ = "item"

    fragment_id: Mapped[str] = mapped_column(
        ForeignKey("fragment.uuid"), primary_key=True
    )
    collection_id: Mapped[int] = mapped_column(
        ForeignKey("collection.id"), primary_key=True
    )
    timestamp: Mapped[str] = mapped_column(DateTime, primary_key=True)
    size: Mapped[int] = mapped_column()
    original_size: Mapped[int] = mapped_column()
    content_type: Mapped[int] = mapped_column()
    hash: Mapped[str] = mapped_column()
