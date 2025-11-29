use common::ProviderReplica;
use sea_orm::sea_query::{OnConflict, TableCreateStatement};
use sea_orm::{
    ColumnTrait, ConnectionTrait, Database, DatabaseConnection, EntityTrait, QueryFilter, Schema,
    Set,
};

use crate::entity::{replica, worker};

pub async fn init(database_url: &str) -> anyhow::Result<DatabaseConnection> {
    let db = Database::connect(database_url).await?;
    apply_schema(
        &db,
        Schema::new(db.get_database_backend()).create_table_from_entity(worker::Entity),
    )
    .await?;
    apply_schema(
        &db,
        Schema::new(db.get_database_backend()).create_table_from_entity(replica::Entity),
    )
    .await?;
    Ok(db)
}

async fn apply_schema(
    db: &DatabaseConnection,
    mut stmt: TableCreateStatement,
) -> anyhow::Result<()> {
    let builder = db.get_database_backend();
    stmt.if_not_exists();
    db.execute(builder.build(&stmt)).await?;
    Ok(())
}

pub async fn upsert_worker(
    db: &DatabaseConnection,
    worker_id: &str,
    approved: bool,
    last_seen: i64,
) -> anyhow::Result<()> {
    let active = worker::ActiveModel {
        worker_id: Set(worker_id.to_string()),
        approved: Set(approved),
        last_seen: Set(last_seen),
    };

    worker::Entity::insert(active)
        .on_conflict(
            OnConflict::column(worker::Column::WorkerId)
                .update_columns([worker::Column::Approved, worker::Column::LastSeen])
                .to_owned(),
        )
        .exec(db)
        .await?;
    Ok(())
}

pub async fn sync_replicas(
    db: &DatabaseConnection,
    worker_id: &str,
    replicas: &[ProviderReplica],
) -> anyhow::Result<()> {
    for replica_data in replicas {
        let active = replica::ActiveModel {
            replica_id: Set(replica_data.replica_id.clone()),
            worker_id: Set(worker_id.to_string()),
            name: Set(replica_data.name.clone()),
            kind: Set(format!("{:?}", replica_data.kind)),
            upstream: Set(replica_data.upstream.clone()),
            status: Set(format!("{:?}", replica_data.status)),
        };

        replica::Entity::insert(active)
            .on_conflict(
                OnConflict::column(replica::Column::ReplicaId)
                    .update_columns([
                        replica::Column::WorkerId,
                        replica::Column::Name,
                        replica::Column::Kind,
                        replica::Column::Upstream,
                        replica::Column::Status,
                    ])
                    .to_owned(),
            )
            .exec(db)
            .await?;
    }

    // Clean up replicas that disappeared from the worker
    let known_ids: Vec<String> = replicas.iter().map(|r| r.replica_id.clone()).collect();
    let mut delete_query =
        replica::Entity::delete_many().filter(replica::Column::WorkerId.eq(worker_id.to_string()));
    if !known_ids.is_empty() {
        delete_query = delete_query.filter(replica::Column::ReplicaId.is_not_in(known_ids));
    }
    delete_query.exec(db).await?;

    Ok(())
}
