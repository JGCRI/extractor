import pandas as pd


class GcamLandclassSplit:
    """Split a GCAM landclass into multiple classes based on the fractional amount present in the observed data per
    subregion.  This method is more desirable than the default "even percentage" split that Demeter conducts.  The
    output file replaces the GCAM target landclass (e.g. RockIceDesert) with the user-selected classes (e.g. snow and
    sparse) per subregion.  The new file becomes what is referenced as the projected file in Demeter.

    :param observed_file:           Full path with file name and extension of the observed data file to be used in the
                                    Demeter run.
    :param projected_file:          Full path with file name and extension of the projected data file that has been
                                    extracted from a GCAM output database for use with Demeter.
    :param target_landclass:        Name of the landclass from the projected file to split (e.g. RockIceDesert).
    :param observed_landclasses:    List of landclass names from the observed data to substitute (e.g. ['snow', 'sparse'].
    :param metric:                  Name of the subregion used. Either 'basin_id' or 'aez_id'.
    :param gcam_year_list:          List of GCAM years to process.
    :param out_file:                Full path with file name and extension for the altered projected data file.

    :return:                        Data frame; save as file
    """
    # region id field name used by Demeter
    REGION_ID_FIELD = 'region_id'
    PRJ_METRIC_ID_FIELD = 'metric_id'
    PRJ_LANDCLASS_FIELD = 'landclass'

    def __init__(self, observed_file, projected_file, target_landclass, observed_landclasses, metric, gcam_year_list,
                 out_file):

        self.observed_file = observed_file
        self.projected_file = projected_file
        self.target_landclass = target_landclass
        self.observed_landclasses = observed_landclasses
        self.metric = metric
        self.gcam_year_list = [str(i) for i in gcam_year_list]
        self.out_file = out_file

        # name of the combined region and subregion field
        self.subregion_field = 'reg_{}'.format(self.metric.split('_')[0])

        # disaggregate landclass
        self.df = self.disaggregate_landclass()

    def calc_observed_fraction(self):
        """Calculate the fraction of land area for each target observed landclass per subregion.

        :return:                    Dictionary; {region_metric_id: (obs_lc, ...), ...}
        """
        cols = [GcamLandclassSplit.REGION_ID_FIELD, self.metric]
        cols.extend(self.observed_landclasses)

        df = pd.read_csv(self.observed_file, usecols=cols)

        # get total amount of observed landclasses in each subregion
        gdf = df.groupby([GcamLandclassSplit.REGION_ID_FIELD, self.metric]).sum(axis=1)
        gdf.reset_index(inplace=True)

        # create key
        gdf[self.subregion_field] = gdf[GcamLandclassSplit.REGION_ID_FIELD].astype(str) + '_' + gdf[self.metric].astype(str)

        # sum observed landclasses (e.g., snow + sparse) per subregion
        gdf['total'] = gdf[self.observed_landclasses].sum(axis=1)

        # create fractional value of each observed landclass per subregion
        frac_lcs = []
        for lc in self.observed_landclasses:
            frac_lc = 'frac_{}'.format(lc)
            frac_lcs.append(frac_lc)

            gdf[frac_lc] = gdf[lc] / gdf['total']

        drop_cols = self.observed_landclasses + ['total', GcamLandclassSplit.REGION_ID_FIELD, self.metric]
        gdf.drop(drop_cols, axis=1, inplace=True)

        # fill subregions that have none of the target values with 0
        gdf.fillna(0, inplace=True)

        return gdf, frac_lcs

    def disaggregate_landclass(self):
        """Disaggregate GCAM target landclass using observed data fraction.  Save output to file.

        :return:                    Data frame
        """

        # get the fractional area from each landclass from the observed data
        obs_df, frac_lcs = self.calc_observed_fraction()

        prj_df = pd.read_csv(self.projected_file)

        # add region_metric field
        prj_df[self.subregion_field] = prj_df[GcamLandclassSplit.REGION_ID_FIELD].astype(str) + '_' + prj_df[GcamLandclassSplit.PRJ_METRIC_ID_FIELD].astype(str)

        # join fractional fields from observed data
        prj_df = pd.merge(prj_df, obs_df, on=self.subregion_field, how='left')

        # data frame containing only the target landclass records
        lc_df = prj_df.loc[prj_df[GcamLandclassSplit.PRJ_LANDCLASS_FIELD] == self.target_landclass].copy()

        # data frame containin ALL but the target landclass records
        out_df = prj_df.loc[prj_df[GcamLandclassSplit.PRJ_LANDCLASS_FIELD] != self.target_landclass].copy()

        for lc in self.observed_landclasses:
            idf = lc_df.copy()

            # set landclass to new field name
            idf[GcamLandclassSplit.PRJ_LANDCLASS_FIELD] = lc

            for yr in self.gcam_year_list:
                idf[yr] *= idf['frac_{}'.format(lc)]

            # add new outputs to data frame
            out_df = pd.concat([idf, out_df])

        # drop processing columns
        frac_lcs.append(self.subregion_field)
        out_df.drop(frac_lcs, axis=1, inplace=True)

        out_df.to_csv(self.out_file, index=False)

        return out_df