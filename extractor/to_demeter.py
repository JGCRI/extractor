import gcam_reader
import numpy as np
import pandas as pd


class GcamToDemeter:
    """Query GCAM database for all land categories leaving irrigated and rainfed use
    as well as management (hi, lo) separated. Format for use by Demeter.

    :param dr_gcam_db:                  Full path to the directory where the GCAM database is stored
    :param f_gcam_db:                   GCAM database name
    :param f_query:                     XML query file for land allocation
    :param f_basin_ref:                 Full path with file name and extension to the input GCAM basin reference file
    :param f_region_ref:                Full path with file name and extension to the input GCAM region reference file
    :param f_out:                       OPTIONAL (default None) Full path with file name and extension to save the output
    :param l_yrs:                       OPTIONAL (default 2010-2100) A list or tuple of desired GCAM years to output
    :param region_name_field:           OPTIONAL (default 'gcam_region_name') Region field name in the GCAM region reference file
    :param region_id_field:             OPTIONAL (default 'gcam_region_id') Region id field name in the GCAM region
                                        reference file
    :param basin_name_field:            OPTIONAL (default 'glu_name') Basin GLU abbreviation field name in the GCAM
                                        basin reference file
    :param basin_id_field:              OPTIONAL (default 'basin_id') Basin id field name in the GCAM basin reference file
    :param output_to_csv:               OPTIONAL (default False) If True, file will be output to the location specified
                                        in the f_out parameter
    :return:                            Data frame; optionally, save as file
    """
    # GCAM naming conventions nested in the GCAM database outputs for land allocation
    GCAM_IRR_NAME = 'IRR'
    GCAM_RFD_NAME = 'RFD'
    GCAM_YEAR_FIELD = 'Year'
    GCAM_UNIT_FIELD = 'Units'
    GCAM_SCENARIO_FIELD = 'scenario'
    GCAM_REGION_FIELD = 'region'
    GCAM_LANDALLOC_FIELD = 'land-allocation'
    GCAM_VALUE_FIELD = 'value'

    # GCAM currently breaks three land classes by an underscore, these are the end position portions of those classes
    GCAM_LANDCLASS_DELIM = '_'
    GCAM_BROKEN_LANDCLASSES = ('Tuber', 'grass', 'tree')

    # Demeter expected naming conventions
    DEMETER_REGID_FIELD = 'region_id'
    DEMETER_LANDCLASS_FIELD = 'landclass'
    DEMETER_METRIC_FIELD = 'metric_id'

    def __init__(self, dr_gcam_db, f_gcam_db, f_query, f_basin_ref, f_region_ref, f_out=None,
                 l_yrs=range(2010, 2105, 5), region_name_field='gcam_region_name', region_id_field='gcam_region_id',
                 basin_name_field='glu_name', basin_id_field='basin_id', output_to_csv=False):

        self.f_basin_ref = f_basin_ref
        self.f_region_ref = f_region_ref
        self.l_yrs = l_yrs
        self.f_out = f_out
        self.region_name_field = region_name_field
        self.region_id_field = region_id_field
        self.basin_name_field = basin_name_field
        self.basin_id_field = basin_id_field
        self.output_to_csv = output_to_csv

        # create db connection
        self.conn = gcam_reader.LocalDBConn(dr_gcam_db, f_gcam_db)

        # get land allocation query
        self.query = gcam_reader.parse_batch_query(f_query)[0]

        # read in basins and regions to lookup dictionaries
        self.d_basins = self.build_basin_dict()
        self.d_regions = self.build_regions_dict()

    @classmethod
    def parse_landclass(cls, x):
        """Parse GCAM land class name into Pandas series.

        :param x:           Pandas GCAM land-allocation series
        :return:            Land class name
        """
        s = x.split(cls.GCAM_LANDCLASS_DELIM)

        if s[1] in cls.GCAM_BROKEN_LANDCLASSES:
            return cls.GCAM_LANDCLASS_DELIM.join(s[:2])
        else:
            return s[0]

    @classmethod
    def parse_basin_name(cls, x):
        """Parse GCAM basin name into Pandas series.

        :param x:           Pandas GCAM land-allocation series
        :return:            Basin GLU abbreviation
        """
        s = x.split(cls.GCAM_LANDCLASS_DELIM)

        if s[1] in cls.GCAM_BROKEN_LANDCLASSES:
            return s[2]
        else:
            return s[1]

    @classmethod
    def parse_use(cls, x):
        """Parse GCAM use type (IRR or RFD) into Pandas series.

        :param x:           Pandas GCAM land-allocation series
        :return:            _IRR, _RFD, or empty string
        """
        s = x.split(cls.GCAM_LANDCLASS_DELIM)

        if cls.GCAM_IRR_NAME in s:
            return '{}{}'.format(cls.GCAM_LANDCLASS_DELIM, cls.GCAM_IRR_NAME)
        elif cls.GCAM_RFD_NAME in s:
            return '{}{}'.format(cls.GCAM_LANDCLASS_DELIM, cls.GCAM_RFD_NAME)
        else:
            return ''

    def build_basin_dict(self):
        """Create a basin lookup dictionary from the input reference file.
        File must be comma-separated with fields titled "basin_id" and "glu_name" for the basin id and
        basin GLU abbreviation, respectively.

        :return:              Dictionary {basin_name: basin_id}
        """
        return pd.read_csv(self.f_basin_ref,
                           usecols=[self.basin_id_field, self.basin_name_field],
                           index_col=self.basin_name_field).to_dict()[self.basin_id_field]

    def build_regions_dict(self):
        """Create a region lookup dictionary from the input reference file.
        File must be comma-separated with fields titled "region_id" and "region" for the region id and
        GCAM region name, respectively.

        :return:              Dictionary {region_name: region_id}
        """
        return pd.read_csv(self.f_region_ref,
                           usecols=[self.region_name_field, self.region_id_field],
                           index_col=self.region_name_field).to_dict()[self.region_id_field]

    def extract_land(self):
        """Extract land allocation data for use in Demeter. Optional:  save to file.

        :return:              Data frame
        """

        df = self.conn.runQuery(self.query)

        # get only target years as defined by the user
        df = df.loc[df[GcamToDemeter.GCAM_YEAR_FIELD].isin(self.l_yrs)]

        df.drop([GcamToDemeter.GCAM_UNIT_FIELD, GcamToDemeter.GCAM_SCENARIO_FIELD], axis=1, inplace=True)

        df[GcamToDemeter.DEMETER_REGID_FIELD] = df[self.GCAM_REGION_FIELD].map(self.d_regions)

        df[GcamToDemeter.DEMETER_LANDCLASS_FIELD] = df[GcamToDemeter.GCAM_LANDALLOC_FIELD].apply(self.parse_landclass)

        # rename region name field to what is specified by the user
        df.rename(columns={GcamToDemeter.GCAM_REGION_FIELD: self.region_name_field}, inplace=True)

        df[self.basin_name_field] = df[GcamToDemeter.GCAM_LANDALLOC_FIELD].apply(self.parse_basin_name)

        df[GcamToDemeter.DEMETER_METRIC_FIELD] = df[self.basin_name_field].map(self.d_basins)

        df['use'] = df[GcamToDemeter.GCAM_LANDALLOC_FIELD].apply(self.parse_use)

        df[GcamToDemeter.DEMETER_LANDCLASS_FIELD] += df['use']

        df.drop([GcamToDemeter.GCAM_LANDALLOC_FIELD, 'use'], axis=1, inplace=True)

        # format for use in Demeter; aggregate hi, low management designation
        piv = pd.pivot_table(df,
                             values=GcamToDemeter.GCAM_VALUE_FIELD,
                             index=[self.region_name_field, self.basin_name_field, GcamToDemeter.DEMETER_REGID_FIELD,
                                    GcamToDemeter.DEMETER_METRIC_FIELD, GcamToDemeter.DEMETER_LANDCLASS_FIELD],
                             columns=GcamToDemeter.GCAM_YEAR_FIELD,
                             fill_value=0,
                             aggfunc=np.sum)

        piv.reset_index(inplace=True)

        if self.output_to_csv:

            if self.f_out is None:
                raise AttributeError('USAGE: Parameter for full path to output file name "f_out" not specified.')

            piv.to_csv(self.f_out, index=False)

        return piv


if __name__ == '__main__':

    import os

    root = '/Users/d3y010/projects/demeter/neal'

    gcam_db_dir = os.path.join(root, 'gcam_outputs')
    gcam_db_name = 'ssp1_rcp60_gfdl'
    query = os.path.join(root, 'reference', 'query_land_reg32_basin235_gcam5p0.xml')
    basin_file = os.path.join(root, 'reference', 'gcam_basin_lookup.csv')
    region_file = os.path.join(root, 'reference', 'gcam_regions_32.csv')

    out_file = '/Users/d3y010/Desktop/test.csv'

    x = GcamToDemeter(gcam_db_dir, gcam_db_name, query, basin_file, region_file, f_out=out_file,
                      l_yrs=range(2010, 2105, 5), region_name_field='gcam_region_name',
                      region_id_field='gcam_region_id', basin_name_field='glu_name', basin_id_field='basin_id',
                      output_to_csv=True)

    x.extract_land()
