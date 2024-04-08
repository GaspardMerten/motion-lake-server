#  Copyright (c) 2024. Gaspard Merten
#  All rights reserved.

from typing import List, Tuple

from sqlalchemy import ForeignKey, DateTime, JSON
from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase, relationship
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

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    uuid: Mapped[str] = mapped_column()
    internal_metadata: Mapped[dict] = mapped_column(JSON, nullable=True)
    collection_id: Mapped[int] = mapped_column(ForeignKey("collection.id"))

    def __repr__(self) -> str:
        return f"Fragment(id={self.id!r}, uuid={self.uuid!r})"


class BufferedFragment(Base):
    __tablename__ = "buffered_fragment"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    collection_id: Mapped[int] = mapped_column(ForeignKey("collection.id"), unique=True)
    segments: Mapped[List[Tuple[int, int, int]]] = mapped_column(JSON)
    fragment_id: Mapped[int] = mapped_column(
        ForeignKey("fragment.id", ondelete="SET NULL"), nullable=True
    )
    associated_fragment: Mapped[Fragment] = relationship("Fragment")

    def __repr__(self) -> str:
        return f"BufferedFragment(id={self.id!r})"


class Item(Base):
    __tablename__ = "item"

    fragment_id: Mapped[int] = mapped_column(ForeignKey("fragment.id"))
    collection_id: Mapped[int] = mapped_column(
        ForeignKey("collection.id"), primary_key=True
    )
    timestamp: Mapped[str] = mapped_column(DateTime, primary_key=True)
