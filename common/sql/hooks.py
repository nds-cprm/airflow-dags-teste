from geopandas import GeoSeries, GeoDataFrame
from typing import Generator, TYPE_CHECKING


if TYPE_CHECKING:
    from airflow.providers.common.sql.hooks.sql import DbApiHook
    BaseClass = DbApiHook
else:
    BaseClass = object


class GeoPandasMixin(BaseClass):
    DEFAULT_SRID = 4326

    def get_geodf_from_geometry(
        self,
        sql,
        parameters=None,
        *,
        geometry_col, 
        crs=DEFAULT_SRID, 
        on_invalid="warn",
        binary=True,
        **kwargs,
    ) -> GeoDataFrame:
        df = self.get_df(sql, parameters, df_type="pandas", **kwargs)
        geom_series = GeoSeries.from_wkb if binary else GeoSeries.from_wkt

        return GeoDataFrame(
            df, 
            geometry=geom_series(
                df.pop(geometry_col), 
                crs=crs, 
                on_invalid=on_invalid
            )
        )

    def get_geodf_from_xy(
            self,
            sql,
            parameters=None,
            *,
            x_col, 
            y_col,
            crs=DEFAULT_SRID,
            pop_coords_from_df=False,
            **kwargs,
        ) -> GeoDataFrame:
            df = self.get_df(sql, parameters, df_type="pandas", **kwargs)
    
            return GeoDataFrame(
                df, 
                geometry=GeoSeries.from_xy(
                    x=df.pop(x_col) if pop_coords_from_df else df[x_col], 
                    y=df.pop(y_col) if pop_coords_from_df else df[y_col], 
                    crs=crs
                )
            )


    def get_geodf_from_geometry_by_chunks(
        self, 
        sql, 
        parameters=None, 
        *,
        chunksize, 
        geometry_col,
        crs=DEFAULT_SRID,
        on_invalid="warn",
        binary=True,
        **kwargs
    ) -> Generator[GeoDataFrame, None, None]:
        df_generator = self.get_df_by_chunks(
            sql=sql,
            parameters=parameters,
            df_type="pandas",
            chunksize=chunksize,
            **kwargs
        )

        for df in df_generator:
            geom_col = df.pop(geometry_col)
            geom_series = GeoSeries.from_wkb if binary else GeoSeries.from_wkt

            yield GeoDataFrame(
                df, 
                geometry=geom_series(
                    data=geom_col, 
                    crs=crs, 
                    on_invalid=on_invalid
                )
            )


    def get_geodf_from_xy_by_chunks(
            self, 
            sql, 
            parameters=None, 
            *
            chunksize, 
            x_col, 
            y_col,
            crs=DEFAULT_SRID,
            pop_coords_from_df=False,
            **kwargs
        ) -> Generator[GeoDataFrame, None, None]:
            df_generator = self.get_df_by_chunks(
                sql=sql,
                parameters=parameters,
                df_type="pandas",
                chunksize=chunksize,
                **kwargs
            )
    
            for df in df_generator:
                yield GeoDataFrame(
                    df, 
                    geometry=GeoSeries.from_xy(
                        x=df.pop(x_col) if pop_coords_from_df else df[x_col], 
                        y=df.pop(y_col) if pop_coords_from_df else df[y_col], 
                        crs=crs
                    )
                )
