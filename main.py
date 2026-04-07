from fastapi.security import HTTPBearer
from fastapi.openapi.models import APIKey, APIKeyIn, SecuritySchemeType
from fastapi.openapi.utils import get_openapi
from fastapi import Depends
from fastapi.security.http import HTTPAuthorizationCredentials

bearer_scheme = HTTPBearer()
from fastapi import FastAPI
from auth import router as auth_router

app = FastAPI()

# Mount routes
app.include_router(auth_router)

@app.get("/")
def home():
    return {"message": "Hotel Labor API running"}
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="LaborLens API",
        version="1.0.0",
        description="API for multi-hotel labor tool with JWT login",
        routes=app.routes,
    )
    openapi_schema["components"]["securitySchemes"] = {
        "BearerAuth": {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "JWT"
        }
    }
    for path in openapi_schema["paths"].values():
        for method in path.values():
            method["security"] = [{"BearerAuth": []}]
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi

if __name__ == "__main__":
    from db import Base, ENGINE
    import db  # This must import your UserAccessControl model

    Base.metadata.create_all(bind=ENGINE)
    print("✅ All missing tables created")
