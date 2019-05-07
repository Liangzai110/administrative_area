# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

# 导入:
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# 创建对象的基类:
Base = declarative_base()


# 定义User对象:
class Area(Base):
    # 表的名字:
    __tablename__ = 'area'

    # 表的结构:
    id = Column(Integer, primary_key=True)
    area_code = Column(String(64))
    level = Column(String(64))
    area_name = Column(String(64))
    full_name = Column(String(64))
    parent = Column(String(64))

    def __repr__(self):
        return "<Area(name='%s', full_name='%s', code='%s', parent='%s', level='%s')>" % (
            self.area_name, self.full_name, self.area_code, self.parent, self.level)


class AdministrativeAreaPipeline(object):

    # 开启爬虫时执行，只执行一次，可以开启数据库等
    def open_spider(self, spider):
        # 初始化数据库连接:
        # echo=Ture----echo 打印执行的SQL语句等较详细的执行信息
        # check_same_thread=False----sqlite让建立的对象任意线程都可使用。
        self.engine = create_engine('sqlite:///test1.db?check_same_thread=False')

        # 创建DBSession类型:
        self.DBSession = sessionmaker(bind=self.engine)

        # 创建 表结构
        Base.metadata.create_all(self.engine)

    # 处理提取的数据(保存数据)
    def process_item(self, item, spider):
        # 由于有了ORM，我们向数据库表中添加一行记录，可以视为添加一个User对象：

        # 创建session对象:
        session = self.DBSession()
        # 创建新User对象:
        new_area = Area(
            area_code=item.get('area_code'),
            area_name=item.get('area_name'),
            level=item.get('level'),
            full_name=item.get('full_name'),
            parent=item.get('parent'),
        )
        print(new_area)

        # 添加到session:
        session.add(new_area)
        # 提交即保存到数据库:
        session.commit()
        # 关闭session:
        session.close()
        return item

    # 关闭爬虫时执行，只执行一次。 (如果爬虫中间发生异常导致崩溃，close_spider可能也不会执行)
    def close_spider(self, spider):
        # 可以关闭数据库等
        pass


if __name__ == '__main__':
    AdministrativeAreaPipeline()
