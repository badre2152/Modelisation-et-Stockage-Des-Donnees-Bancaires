import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from database_pipeline_py.dash_app.utils.db import get_db_engine
from sqlalchemy import text

engine = get_db_engine()

# تحقق من الجداول الموجودة
inspector = __import__('sqlalchemy').inspect(engine)
tables = inspector.get_table_names()
print("📋 الجداول الموجودة:")
print(tables)

# تحقق من عدد الصفوف في كل جدول
with engine.connect() as conn:
    for table in tables:
        try:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
            count = result.scalar()
            print(f"\n {table}: {count} صفوف")
        except Exception as e:
            print(f"\n❌ {table}: خطأ — {e}")