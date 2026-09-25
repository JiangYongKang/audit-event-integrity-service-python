"""FastAPI 路由（骨架）。"""
from __future__ import annotations

from fastapi import FastAPI

from app.service import AuditService


def create_app(service: AuditService | None = None) -> FastAPI:
    raise NotImplementedError
