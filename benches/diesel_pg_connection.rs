#[macro_use]
extern crate diesel;
use criterion::{criterion_group, criterion_main};
use criterion::{BenchmarkId, Criterion, Throughput};

table! {
    users {
        id -> Integer,
        name -> Text,
        hair_color -> Nullable<Text>,
    }
}

table! {
    posts {
        id -> Integer,
        user_id -> Integer,
        title -> Text,
        body -> Nullable<Text>,
    }
}

joinable!(posts -> users(user_id));
allow_tables_to_appear_in_same_query!(users, posts);

use diesel::*;
use std::env;

#[derive(Queryable, Identifiable)]
pub struct User {
    id: i32,
    name: String,
    hair_color: Option<String>,
}

#[derive(Insertable)]
#[table_name = "users"]
struct NewUser {
    name: String,
    hair_color: Option<String>,
}

#[derive(Queryable, Identifiable, Associations)]
#[belongs_to(User)]
struct Post {
    id: i32,
    user_id: i32,
    title: String,
    body: Option<String>,
}

#[derive(Insertable)]
#[table_name = "posts"]
struct NewPost<'a> {
    user_id: i32,
    title: String,
    body: Option<&'a str>,
}

fn pg_connection() -> PgConnection {
    let database_url = env::var("DATABASE_URL").unwrap();
    let conn = PgConnection::establish(&database_url).unwrap();
    conn.execute("DELETE FROM posts").unwrap();
    conn.execute("DELETE FROM users").unwrap();
    conn.execute("alter sequence users_id_seq RESTART WITH 1")
        .unwrap();
    conn.execute("alter sequence posts_id_seq RESTART WITH 1")
        .unwrap();
    conn
}

fn postgres_connection() -> PostgresConnection {
    let database_url = env::var("DATABASE_URL").unwrap();
    let conn = PostgresConnection::establish(&database_url).unwrap();
    conn.execute("DELETE FROM posts").unwrap();
    conn.execute("DELETE FROM users").unwrap();
    conn.execute("alter sequence users_id_seq RESTART WITH 1")
        .unwrap();
    conn.execute("alter sequence posts_id_seq RESTART WITH 1")
        .unwrap();
    conn
}

fn benchmark_simple_query(b: &mut Criterion) {
    let mut group = b.benchmark_group("simple_query");

    for num_rows in &[0, 1, 10, 100, 1_000, 10_000] {
        let num_rows = *num_rows;
        let pg_conn = pg_connection();
        let postgres_conn = postgres_connection();

        let data: Vec<_> = (0..num_rows)
            .map(|i| NewUser {
                name: format!("User {}", i),
                hair_color: None,
            })
            .collect();
        assert_eq!(
            Ok(num_rows),
            insert_into(users::table).values(&data).execute(&pg_conn)
        );

        group.throughput(Throughput::Elements(num_rows as u64));
        group.bench_with_input(
            BenchmarkId::new("LibPq", num_rows),
            &num_rows,
            |b, &num_rows| {
                b.iter(|| {
                    assert_eq!(num_rows, users::table.load::<User>(&pg_conn).unwrap().len());
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("NativeRustPostgres", num_rows),
            &num_rows,
            |b, &num_rows| {
                b.iter(|| {
                    assert_eq!(
                        num_rows,
                        users::table.load::<User>(&postgres_conn).unwrap().len()
                    );
                })
            },
        );
    }
    group.finish();
}

fn benchmark_complex_query(b: &mut Criterion) {
    let mut group = b.benchmark_group("complex_query");
    for num_rows in &[0, 1, 10, 100, 1_000] {
        let num_rows = *num_rows;
        let pg_conn = pg_connection();
        let postgres_conn = postgres_connection();

        let mut posts = Vec::new();
        let data: Vec<_> = (0..num_rows)
            .map(|i| {
                let hair_color = if i % 2 == 0 { "black" } else { "brown" };
                let user = NewUser {
                    name: format!("User {}", i),
                    hair_color: Some(hair_color.into()),
                };

                if i % 3 == 0 {
                    posts.push(NewPost {
                        user_id: i as i32 + 1,
                        title: format!("My {}. post", i),
                        body: Some("This is the body of my first post"),
                    })
                }
                user
            })
            .collect();
        assert_eq!(
            Ok(num_rows),
            insert_into(users::table).values(&data).execute(&pg_conn)
        );
        assert_eq!(
            Ok(posts.len()),
            insert_into(posts::table).values(&posts).execute(&pg_conn)
        );

        group.throughput(Throughput::Elements(num_rows as u64));
        group.bench_with_input(
            BenchmarkId::new("LibPq", num_rows),
            &num_rows,
            |b, &num_rows| {
                b.iter(|| {
                    let query = users::table
                        .left_outer_join(posts::table)
                        .filter(users::hair_color.eq("black"))
                        .order(users::name.desc());
                    let expected_row_count = (num_rows as f64 / 2.0).ceil() as usize;
                    assert_eq!(
                        expected_row_count,
                        query.load::<(User, Option<Post>)>(&pg_conn).unwrap().len()
                    );
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("NativeRustPostgres", num_rows),
            &num_rows,
            |b, &num_rows| {
                b.iter(|| {
                    let query = users::table
                        .left_outer_join(posts::table)
                        .filter(users::hair_color.eq("black"))
                        .order(users::name.desc());
                    let expected_row_count = (num_rows as f64 / 2.0).ceil() as usize;
                    assert_eq!(
                        expected_row_count,
                        query
                            .load::<(User, Option<Post>)>(&postgres_conn)
                            .unwrap()
                            .len()
                    );
                })
            },
        );
    }
    group.finish();
}

criterion_group!(benches, benchmark_simple_query, benchmark_complex_query);
criterion_main!(benches);
