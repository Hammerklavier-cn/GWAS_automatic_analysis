use polars::prelude::*;

pub fn standardize(lf: LazyFrame, iid_index: i64) -> PolarsResult<LazyFrame> {
    let contents_lf = lf.clone().drop(nth(iid_index));

    let res_lf = lf
        .clone()
        .select([nth(iid_index).as_expr().alias("FID")])
        .with_column(col("FID").alias("IID"))
        .collect()?
        .hstack(contents_lf.collect()?.get_columns())?
        .lazy();

    Ok(res_lf)
}
