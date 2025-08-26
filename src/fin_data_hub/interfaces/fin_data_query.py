from fastapi import APIRouter

router = APIRouter(
    prefix="/fin-data",
    tags=["金融数据查询接口"],
)

@router.get(
    "/query",
    summary="查询金融数据",
    description="根据条件查询金融数据",
)
async def query_fin_data(
    query: str,
    page: int = 1,
    page_size: int = 10,
):
    return {"message": "Hello, World!"}

