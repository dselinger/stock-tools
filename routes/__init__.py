from fastapi import APIRouter

from routes.debug import router as debug_router
from routes.events import router as events_router

router = APIRouter()

# Compose sub-routers
router.include_router(debug_router)
router.include_router(events_router)
