#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlalchemy import Column, Integer, String, Boolean
import sqlalchemy
from sqlalchemy.dialects.postgresql import JSON, JSONB
from sqlalchemy.ext.declarative import declarative_base

from exdemon.config import config

Base = declarative_base()

class Schema(Base):
    __tablename__ = 'schema'

    id = Column('id', Integer, primary_key=True, autoincrement=True)
    name = Column('name', String(32), nullable=False)
    project = Column('project', String(32), nullable=False)
    environment = Column('environment', String(32), nullable=False)
    data = Column('data', JSON, nullable=False)
    enabled = Column('enabled', Boolean, nullable=False)

    def __repr__(self):
        return "<Schema(id='%s', name='%s', project='%s', environment='%s', 'data='%s')>" % (
                        self.id, self.name, self.project, self.environment, self.data)

class Metric(Base):
    __tablename__ = 'metric'

    id = Column('id', Integer, primary_key=True, autoincrement=True)
    name = Column('name', String(32), nullable=False)
    project = Column('project', String(32), nullable=False)
    environment = Column('environment', String(32), nullable=False)
    data = Column('data', JSON, nullable=False)
    enabled = Column('enabled', Boolean, nullable=False)

    def __repr__(self):
        return "<Metric(id='%s', name='%s', project='%s', environment='%s', 'data='%s')>" % (
                        self.id, self.name, self.project, self.environment, self.data)

class Monitor(Base):
    __tablename__ = 'monitor'

    id = Column('id', Integer, primary_key=True, autoincrement=True)
    name = Column('name', String(32), nullable=False)
    project = Column('project', String(32), nullable=False)
    environment = Column('environment', String(32), nullable=False)
    data = Column('data', JSON, nullable=False)
    enabled = Column('enabled', Boolean, nullable=False)

    def __repr__(self):
        return "<Monitor(id='%s', name='%s', project='%s', environment='%s', 'data='%s')>" % (
                        self.id, self.name, self.project, self.environment, self.data)

if __name__ == "__main__":
    # Create the database schema
    connection_string = config.get('database', 'connection')
    db = sqlalchemy.create_engine(connection_string)  
    engine = db.connect()  
    Base.metadata.create_all(engine)

