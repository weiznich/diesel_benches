#[macro_use]
extern crate diesel;
use criterion::{criterion_group, criterion_main};
use criterion::{BenchmarkId, Criterion, Throughput};
use postgres::Client;

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

fn raw_sql_connection() -> Client {
    let database_url = env::var("DATABASE_URL").unwrap();
    let mut conn = Client::connect(&database_url, postgres::tls::NoTls).unwrap();
    conn.simple_query("DELETE FROM posts").unwrap();
    conn.simple_query("DELETE FROM users").unwrap();
    conn.simple_query("alter sequence users_id_seq RESTART WITH 1")
        .unwrap();
    conn.simple_query("alter sequence posts_id_seq RESTART WITH 1")
        .unwrap();
    conn
}

fn benchmark_simple_query(b: &mut Criterion) {
    let mut group = b.benchmark_group("simple_query");

    for num_rows in &[0, 1, 10, 100, 1_000, 10_000] {
        let num_rows = *num_rows;
        let pg_conn = pg_connection();
        let postgres_conn = postgres_connection();
        let mut raw_sql_conn = raw_sql_connection();

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
            BenchmarkId::new("diesel-libpq", num_rows),
            &num_rows,
            |b, &num_rows| {
                b.iter(|| {
                    assert_eq!(num_rows, users::table.load::<User>(&pg_conn).unwrap().len());
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("diesel-native-postgres", num_rows),
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

        group.bench_with_input(
            BenchmarkId::new("postgres-naive", num_rows),
            &num_rows,
            |b, &num_rows| {
                b.iter(|| {
                    use postgres::fallible_iterator::FallibleIterator;

                    let users = raw_sql_conn
                        .query_raw("SELECT id, name, hair_color FROM users", vec![])
                        .unwrap()
                        .map(|row| {
                            Ok(User {
                                id: row.get("id"),
                                name: row.get("name"),
                                hair_color: row.get("hair_color"),
                            })
                        })
                        .collect::<Vec<_>>()
                        .unwrap();

                    assert_eq!(num_rows, users.len());
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("postgres-optimized", num_rows),
            &num_rows,
            |b, &num_rows| {
                let mut statement = None;
                b.iter(|| {
                    use postgres::fallible_iterator::FallibleIterator;

                    let statement = if let Some(statement) = statement.as_ref() {
                        statement
                    } else {
                        statement = Some(
                            raw_sql_conn
                                .prepare("SELECT id, name, hair_color FROM users")
                                .unwrap(),
                        );
                        statement.as_ref().unwrap()
                    };

                    let users = raw_sql_conn
                        .query_raw(statement, vec![])
                        .unwrap()
                        .map(|row| {
                            Ok(User {
                                id: row.get(0),
                                name: row.get(1),
                                hair_color: row.get(2),
                            })
                        })
                        .collect::<Vec<_>>()
                        .unwrap();

                    assert_eq!(num_rows, users.len());
                });
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
        let mut raw_sql_conn = raw_sql_connection();

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
            BenchmarkId::new("diesel-libpq", num_rows),
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
            BenchmarkId::new("diesel-native-postgres", num_rows),
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

        group.bench_with_input(
            BenchmarkId::new("postgres-naive", num_rows),
            &num_rows,
            |b, &num_rows| {
                b.iter(|| {
                    use postgres::fallible_iterator::FallibleIterator;

                    let user_and_posts = raw_sql_conn
                        .query_raw(
                            "SELECT users.id as user_id, users.name as user_name, users.hair_color as user_hair_color,\
                                 posts.id as post_id, posts.user_id as post_user_id, posts.title as post_title, posts.body as post_body
                             FROM users \
                             LEFT OUTER JOIN posts ON posts.user_id = users.id
                             WHERE users.hair_color = $1 \
                             ORDER BY users.name DESC",
                            vec![&"black" as _],
                        )
                        .unwrap()
                        .map(|row| {
                            let user = User {
                                id: row.get("user_id"),
                                name: row.get("user_name"),
                                hair_color: row.get("user_hair_color"),
                            };
                            let post_id: Option<i32> = row.get("post_id");
                            let post = post_id.map(|id| {
                                Post{
                                    id,
                                    user_id: row.get("post_user_id"),
                                    title: row.get("post_title"),
                                    body: row.get("post_body"),
                                }
                            });
                            Ok((user, post))
                        })
                        .collect::<Vec<_>>()
                        .unwrap();
                    let expected_row_count = (num_rows as f64 / 2.0).ceil() as usize;
                    assert_eq!(
                        expected_row_count,
                        user_and_posts.len()
                    );
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("postgres-optimized", num_rows),
            &num_rows,
            |b, &num_rows| {
                let mut statement = None;
                b.iter(|| {
                    use postgres::fallible_iterator::FallibleIterator;
                    use postgres::types::Type;

                    let statement = if let Some(statement) = statement.as_ref() {
                        statement
                    } else {
                        statement = Some(
                            raw_sql_conn
                                .prepare_typed("SELECT users.id as user_id, users.name as user_name, users.hair_color as user_hair_color,\
                                 posts.id as post_id, posts.user_id as post_user_id, posts.title as post_title, posts.body as post_body
                             FROM users \
                             LEFT OUTER JOIN posts ON posts.user_id = users.id
                             WHERE users.hair_color = $1 \
                             ORDER BY users.name DESC", &[Type::TEXT])
                                .unwrap(),
                        );
                        statement.as_ref().unwrap()
                    };
                    let user_and_posts = raw_sql_conn
                        .query_raw(statement, vec![&"black" as _])
                        .unwrap()
                        .map(|row| {
                            let user = User {
                                id: row.get("user_id"),
                                name: row.get("user_name"),
                                hair_color: row.get("user_hair_color"),
                            };
                            let post_id: Option<i32> = row.get("post_id");
                            let post = post_id.map(|id| Post {
                                id,
                                user_id: row.get("post_user_id"),
                                title: row.get("post_title"),
                                body: row.get("post_body"),
                            });
                            Ok((user, post))
                        })
                        .collect::<Vec<_>>()
                        .unwrap();
                    let expected_row_count = (num_rows as f64 / 2.0).ceil() as usize;
                    assert_eq!(expected_row_count, user_and_posts.len());
                });
            },
        );
    }
    group.finish();
}

fn benchmark_batch_insert(b: &mut Criterion) {
    let mut group = b.benchmark_group("batch_insert");

    for num_rows in &[1, 10, 25, 50, 100] {
        let num_rows = *num_rows;
        let pg_conn = pg_connection();
        let postgres_conn = postgres_connection();
        let mut raw_sql_conn = raw_sql_connection();

        let data: Vec<_> = (0..num_rows)
            .map(|i| NewUser {
                name: format!("User {}", i),
                hair_color: None,
            })
            .collect();

        group.throughput(Throughput::Elements(num_rows as u64));
        group.bench_with_input(
            BenchmarkId::new("diesel-libpq", num_rows),
            &num_rows,
            |b, &num_rows| {
                b.iter(|| {
                    assert_eq!(
                        Ok(num_rows),
                        insert_into(users::table).values(&data).execute(&pg_conn)
                    )
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("diesel-native-postgres", num_rows),
            &num_rows,
            |b, &num_rows| {
                b.iter(|| {
                    assert_eq!(
                        Ok(num_rows),
                        insert_into(users::table)
                            .values(&data)
                            .execute(&postgres_conn)
                    )
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("postgres-naive", num_rows),
            &num_rows,
            |b, &num_rows| {
                b.iter(|| {
                    use postgres::types::ToSql;

                    let mut query = String::from("INSERT INTO users (name, hair_color) VALUES ");
                    let mut first = true;
                    let mut binds = Vec::new();
                    for (i, d) in data.iter().enumerate() {
                        if first {
                            first = false;
                        } else {
                            query += ", ";
                        };
                        query += &format!("(${}, ${})", 2 * i + 1, 2 * i + 2);
                        binds.push(&d.name as &(dyn ToSql + Sync));
                        binds.push(&d.hair_color as &(dyn ToSql + Sync));
                    }

                    assert_eq!(
                        num_rows as u64,
                        raw_sql_conn
                            .execute(&query as &str, &binds as &[_])
                            .unwrap()
                    )
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("postgres-optimized", num_rows),
            &num_rows,
            |b, &num_rows| {
                let mut statement = None;
                b.iter(|| {
                    use postgres::types::{ToSql, Type};

                    let statement = if let Some(statement) = statement.as_ref() {
                        statement
                    } else {
                        let mut query =
                            String::from("INSERT INTO users (name, hair_color) VALUES ");
                        let mut types = Vec::with_capacity(data.len() * 2);
                        let mut first = true;
                        for i in 0..num_rows {
                            if first {
                                first = false;
                            } else {
                                query += ", ";
                            };
                            query += "($";
                            query += &(2 * i + 1).to_string();
                            query += ", $";
                            query += &(2 * i + 2).to_string();
                            query += ")";
                            types.push(Type::TEXT);
                            types.push(Type::TEXT);
                        }
                        statement = Some(raw_sql_conn.prepare_typed(&query, &types).unwrap());
                        statement.as_ref().unwrap()
                    };

                    let mut binds = Vec::with_capacity(2 * data.len());
                    for d in &data {
                        binds.push(&d.name as &(dyn ToSql + Sync));
                        binds.push(&d.hair_color as &(dyn ToSql + Sync));
                    }

                    assert_eq!(
                        num_rows as u64,
                        raw_sql_conn.execute(statement, &binds as &[_]).unwrap()
                    )
                })
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    benchmark_simple_query,
    benchmark_complex_query,
    benchmark_batch_insert
);
criterion_main!(benches);
