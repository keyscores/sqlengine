#Interface for the front end to create reports


def measure_data(company_id, measures, frequency, start_date, end_date,
    groupby=None, measure_filter=None, dimension_filters=None, score_kpis=True):
    """
    measures - set() of measure IDs
    frequency - str of 'month', 'day' or 'quarter'
    start_date - datetime.datetime object, use data >=this
    end_date - datetime.datetime object, use data <this
    groupby=None - str dimension name or None
    measure_filter=None - if passed, will be a set() of measure ID
    dimension_filters=None - dictionary with dimension name keys
                              the values are a data structure meant to represent
                              a logical expression on what levels (like a WHERE clause)
                              e.g.
                              {
                                  'DIM1':'LEV1'
                                  'DIM2':('|', 'LEV5', 'LEV6'),
                                  'DIM3':('&', 'LEV10', 'LEV11'),
                              }

                              The above is equivalent to this simplified WHERE clause:
                              WHERE DIM1 = LEV1 AND (DIM2 = LEV5 OR DIM2 = LEV6)
                                      AND (DIM3 = LEV10 AND DIM3 = LEV11)

                              The DIM3 part will look strange, but the user may request
                              this. With current data this will return no rows, but the
                              fact table structure does support this case so it should
                              be accessible.

                              If a None is passed then don't do filtering on dimensions

    score_kpis=True - if True, return 1-10 scores for KPIs, if False return values

    Return Value:

    Coming soon...
    """
    pass
