#[cfg(test)]
mod destination_delta {
    use aqueducts::prelude::*;
    use datafusion::{
        assert_batches_eq,
        execution::context::SessionContext,
        logical_expr::{col, lit},
    };
    use std::{collections::HashMap, path::Path, sync::Arc};

    #[test]
    fn test_try_from_yml_ok() {
        let local_path = Path::new("../../")
            .canonicalize()
            .unwrap()
            .into_os_string()
            .into_string()
            .unwrap();
        let params = HashMap::from_iter(vec![
            ("local_path".into(), format!("file:///{local_path}")),
            ("run_id".into(), "12345678".into()),
        ]);

        Aqueduct::try_from_yml("../../examples/aqueduct_pipeline_example.yml", params).unwrap();
    }

    #[test]
    fn test_try_from_json_ok() {
        let params = HashMap::from_iter(vec![
            ("local_path".into(), "file:///path1".into()),
            ("run_id".into(), "12345678".into()),
            ("date".into(), "1970-01-01".into()),
        ]);

        Aqueduct::try_from_json("../../examples/aqueduct_pipeline_example.json", params).unwrap();
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_run_pipeline_append_ok() {
        // setup
        let local_path = Path::new(".")
            .canonicalize()
            .unwrap()
            .into_os_string()
            .into_string()
            .unwrap();

        let run_id = rand::random::<usize>().to_string();
        let params = HashMap::from_iter(vec![
            ("local_path".into(), format!("file://{local_path}")),
            ("run_id".into(), run_id.clone()),
            ("date".into(), "2023-05-01".into()),
        ]);
        let pipeline =
            Aqueduct::try_from_yml("./tests/data/aqueduct_pipeline_delta_append.yml", params)
                .unwrap();

        // test

        // 1st run
        let _ = run_pipeline(pipeline.clone(), None).await.unwrap();

        // 2nd run
        let _ = run_pipeline(pipeline, None).await.unwrap();

        // assert
        let output_table =
            deltalake::open_table(format!("./tests/output/test_delta_append/{run_id}"))
                .await
                .unwrap();

        let ctx = SessionContext::new();
        let count = ctx
            .read_table(Arc::new(output_table))
            .unwrap()
            .count()
            .await
            .unwrap();

        let expected_count = 52;
        assert_eq!(expected_count, count);
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_run_pipeline_replace_ok() {
        // setup
        let local_path = Path::new(".")
            .canonicalize()
            .unwrap()
            .into_os_string()
            .into_string()
            .unwrap();
        let run_id = rand::random::<usize>().to_string();
        let params = HashMap::from_iter(vec![
            ("local_path".into(), format!("file://{local_path}")),
            ("run_id".into(), run_id.clone()),
            ("date".into(), "2023-05-02".into()),
        ]);
        let pipeline =
            Aqueduct::try_from_yml("./tests/data/aqueduct_pipeline_delta_replace.yml", params)
                .unwrap();

        // test

        // 1st run
        let _ = run_pipeline(pipeline.clone(), None).await.unwrap();

        // 2nd run
        let _ = run_pipeline(pipeline, None).await.unwrap();

        // assert
        let output_table =
            deltalake::open_table(format!("./tests/output/test_delta_replace/{run_id}"))
                .await
                .unwrap();

        let ctx = SessionContext::new();
        let top_10 = ctx
            .read_table(Arc::new(output_table))
            .unwrap()
            .select_columns(&["date", "country", "sum_1", "sum_2", "avg_1", "avg_2"])
            .unwrap()
            .filter(col("date").eq(lit("2023-05-02")))
            .unwrap()
            .sort(vec![
                col("sum_1").sort(false, true),
                col("sum_2").sort(false, true),
            ])
            .unwrap()
            .limit(0, Some(10))
            .unwrap();

        assert_batches_eq!(&[
            "+------------+---------+-------+--------------------+--------------------+-------------------+",
            "| date       | country | sum_1 | sum_2              | avg_1              | avg_2             |",
            "+------------+---------+-------+--------------------+--------------------+-------------------+",
            "| 2023-05-02 | CN      | 1070  | 1119.0400000000004 | 40.44444444444444  | 48.29444444444445 |",
            "| 2023-05-02 | PH      | 558   | 731.9699999999999  | 57.0               | 39.90833333333334 |",
            "| 2023-05-02 | RU      | 395   | 209.93             | 59.333333333333336 | 40.82555555555555 |",
            "| 2023-05-02 | BR      | 378   | 334.46000000000004 | 23.25              | 60.47             |",
            "| 2023-05-02 | US      | 275   | 370.14             | 53.0               | 87.8925           |",
            "| 2023-05-02 | ID      | 222   | 252.10000000000002 | 47.2               | 48.86533333333332 |",
            "| 2023-05-02 | FR      | 204   | 264.07             | 24.0               | 16.76             |",
            "| 2023-05-02 | CA      | 202   | 185.99             | 20.5               | 27.175            |",
            "| 2023-05-02 | CZ      | 146   | 88.28              | 57.666666666666664 | 41.64666666666667 |",
            "| 2023-05-02 | MY      | 140   | 98.63              | 50.0               | 85.87             |",
            "+------------+---------+-------+--------------------+--------------------+-------------------+",
        ], top_10.collect().await.unwrap().as_slice());
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_run_pipeline_upsert_ok() {
        // setup
        let local_path = Path::new(".")
            .canonicalize()
            .unwrap()
            .into_os_string()
            .into_string()
            .unwrap();
        let run_id = rand::random::<usize>().to_string();
        let params = HashMap::from_iter(vec![
            ("local_path".into(), format!("file://{local_path}")),
            ("run_id".into(), run_id.clone()),
            ("date".into(), "2023-05-03".into()),
        ]);
        let pipeline =
            Aqueduct::try_from_yml("./tests/data/aqueduct_pipeline_delta_upsert.yml", params)
                .unwrap();

        // test

        // 1st run
        let _ = run_pipeline(pipeline.clone(), None).await.unwrap();

        // 2nd run
        let _ = run_pipeline(pipeline, None).await.unwrap();

        // assert
        let output_table =
            deltalake::open_table(format!("./tests/output/test_delta_upsert/{run_id}"))
                .await
                .unwrap();

        let ctx = SessionContext::new();
        let top_10 = ctx
            .read_table(Arc::new(output_table))
            .unwrap()
            .select_columns(&["date", "country", "sum_1", "sum_2", "avg_1", "avg_2"])
            .unwrap()
            .filter(col("date").eq(lit("2023-05-03")))
            .unwrap()
            .sort(vec![
                col("sum_1").sort(false, true),
                col("sum_2").sort(false, true),
            ])
            .unwrap()
            .limit(0, Some(10))
            .unwrap();

        assert_batches_eq!(&[
            "+------------+---------+-------+--------------------+--------------------+--------------------+",
            "| date       | country | sum_1 | sum_2              | avg_1              | avg_2              |",
            "+------------+---------+-------+--------------------+--------------------+--------------------+",
            "| 2023-05-03 | CN      | 1260  | 1172.5200000000002 | 40.27272727272727  | 53.38727272727272  |",
            "| 2023-05-03 | RU      | 350   | 200.33999999999997 | 44.714285714285715 | 43.660000000000004 |",
            "| 2023-05-03 | PH      | 317   | 447.85             | 46.5               | 76.17500000000001  |",
            "| 2023-05-03 | ID      | 294   | 356.27000000000004 | 41.09090909090909  | 37.03272727272727  |",
            "| 2023-05-03 | BR      | 289   | 331.99             | 68.57142857142857  | 63.67857142857144  |",
            "| 2023-05-03 | PL      | 273   | 381.25             | 59.25              | 52.237500000000004 |",
            "| 2023-05-03 | SE      | 223   | 73.61              | 51.0               | 14.96              |",
            "| 2023-05-03 | PE      | 220   | 231.95999999999998 | 21.5               | 41.94              |",
            "| 2023-05-03 | CA      | 194   | 108.69             | 66.0               | 62.4               |",
            "| 2023-05-03 | US      | 183   | 129.14999999999998 | 60.666666666666664 | 30.643333333333334 |",
            "+------------+---------+-------+--------------------+--------------------+--------------------+",
        ], top_10.collect().await.unwrap().as_slice());
    }
}
