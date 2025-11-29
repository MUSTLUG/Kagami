use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "replicas")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub replica_id: String,
    pub worker_id: String,
    pub name: String,
    pub kind: String,
    pub upstream: String,
    pub status: String,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::worker::Entity",
        from = "Column::WorkerId",
        to = "super::worker::Column::WorkerId"
    )]
    Worker,
}

impl Related<super::worker::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Worker.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
