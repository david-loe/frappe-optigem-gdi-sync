from pydantic import BaseModel, Field, model_validator
from typing import Annotated, Literal, Union, Optional


class DatabaseBase(BaseModel):
    database: str
    user: str
    password: str


class MssqlDatabaseConfig(DatabaseBase):
    type: Literal["mssql"]
    server: str
    trust_server_certificate: bool = False


class FirebirdDatabaseConfig(DatabaseBase):
    type: Literal["firebird"]
    host: str
    port: int
    charset: str = "UTF8"


DatabaseConfig = Annotated[Union[MssqlDatabaseConfig, FirebirdDatabaseConfig], Field(discriminator="type")]


class FrappeAuthConfig(BaseModel):
    api_key: str
    api_secret: str


class FrappeConfig(FrappeAuthConfig):
    limit_page_length: int = 20


class TaskFrappeConfig(BaseModel):
    modified_field: str
    fk_id_field: str


class TaskDbConfig(BaseModel):
    modified_field: str
    fk_id_field: str
    fallback_modified_field: Optional[str] = None


class TaskBase(BaseModel):
    name: Optional[str] = None
    endpoint: str
    db_name: str
    mapping: dict[str, str]
    key_fields: list[str]
    table_name: Optional[str] = None
    query: Optional[str] = None
    process_all: bool = False
    create_new: bool = False

    @model_validator(mode="after")
    def check_key_fields_in_mapping(self) -> "TaskBase":
        # Wir nutzen hier die in der TaskMapping definierten Felder
        mapping_keys = self.mapping.keys()
        missing_keys = [key for key in self.key_fields if key not in mapping_keys]
        if missing_keys:
            raise ValueError(f"Die folgenden key_fields fehlen im mapping: {missing_keys}")
        return self


class BidirectionalTaskConfig(TaskBase):
    direction: Literal["bidirectional"]
    name: str = "Bidirectional Sync"
    table_name: str
    frappe: TaskFrappeConfig
    db: TaskDbConfig


class DbToFrappeTaskConfig(TaskBase):
    direction: Literal["db_to_frappe"]
    name: str = "DB -> Frappe"

    @model_validator(mode="after")
    def validate_table_or_query(self) -> "DbToFrappeTaskConfig":
        if self.table_name is None and self.query is None:
            raise ValueError("Entweder 'table_name' oder 'query' muss angegeben werden.")
        return self


class FrappeToDbTaskConfig(TaskBase):
    direction: Literal["frappe_to_db"]
    name: str = "Frappe -> DB"
    table_name: str


TaskConfig = Annotated[
    Union[BidirectionalTaskConfig, DbToFrappeTaskConfig, FrappeToDbTaskConfig], Field(discriminator="direction")
]


class Config(BaseModel):
    dry_run: bool = False
    databases: dict[str, DatabaseConfig]
    frappe: FrappeConfig
    tasks: list[TaskConfig]
