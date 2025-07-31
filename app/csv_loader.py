import pandas as pd
import asyncio
from sqlalchemy import select
from app.database import AsyncSessionLocal as SessionLocal, engine
from app.models import Product, Department, Base

PRODUCTS_CSV = "products.csv"

async def load_data():
    # Create tables if they don't exist
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    df = pd.read_csv(PRODUCTS_CSV)

    # Drop rows where 'name' or 'department' is missing
    df = df.dropna(subset=['name', 'department'])

    async with SessionLocal() as session:
        for _, row in df.iterrows():
            # Get or create department
            result = await session.execute(
                select(Department.id).where(Department.name == row['department'])
            )
            dept = result.scalar()

            if not dept:
                new_department = Department(name=row['department'])
                session.add(new_department)
                await session.flush()
                department_id = new_department.id
            else:
                department_id = dept

            # Create product
            product = Product(
                id=row['id'],
                name=str(row['name']),
                price=float(row['cost']) if not pd.isna(row['cost']) else 0.0,
                stock=100,
                department_id=department_id
            )
            session.add(product)

        await session.commit()

if __name__ == "__main__":
    asyncio.run(load_data())
