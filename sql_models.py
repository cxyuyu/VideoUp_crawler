from sqlalchemy import Column, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import INTEGER, BIGINT, DOUBLE, TINYINT, CHAR, VARCHAR, TEXT, TIMESTAMP
from local_config import mysql_config

BaseModel = declarative_base()  # 创建了一个 BaseModel 类，这个类的子类可以自动与一个表关联。


class apps_version_record(BaseModel):
    __tablename__ = 'apps_version_record'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8mb4',
        'schema': 'raw_db'
    }
    app_id = Column(VARCHAR(100), nullable=False,primary_key=True)
    app_name = Column(VARCHAR(100), nullable=True)
    publisher = Column(VARCHAR(100), nullable=True)
    version = Column(VARCHAR(100), nullable=True)
    size = Column(VARCHAR(100), nullable=True)
    category = Column(VARCHAR(100), nullable=True)
    rate = Column(VARCHAR(100), nullable=True)
    note = Column(VARCHAR(1000), nullable=True)
    url = Column(VARCHAR(1000), nullable=True)
    update_time = Column(TIMESTAMP(6), nullable=False)




class DB():
    def __init__(self):
        self.session = self.DBSession()

    def DBSession(self):
        # 初始化数据库连接:
        db_uri = 'mysql+pymysql://{username}:{password}@{host}:3306'.format(username=mysql_config['username'],
                                                                            password=mysql_config['password'],
                                                                            host=mysql_config['host'])
        engine = create_engine(db_uri, pool_size=5, pool_recycle=7200, pool_pre_ping=True, max_overflow=-1)
        # 创建DBSession类型:
        DBSession = sessionmaker(bind=engine)
        DBSession = DBSession()
        return DBSession

    def __del__(self):
        self.session.close()


if __name__ == "__main__":
    session = DB().session

    sql = 'select count(*) from raw_db.awhile_instagram_parenting '
    cursor = session.execute(sql)
    print(cursor.fetchall()[0][0])
    session.commit()
    exit()

    # # 创建新User对象:
    # import datetime
    # print(datetime.datetime.now())
    # new_user = amino_family(family='Bob', hot=1, update_time=str(datetime.datetime.now()),category='asdfadf')
    # # new_user = amino_article(family='Bob',author_url='asdfadf' , update_time='2018-11-09')
    # # 添加到session:
    # session.add(new_user)

    # 提交即保存到数据库:
    session.commit()
    # 关闭session:
    session.close()
