from ks_precompute import precompute
from ks_analytics import analytics
#Interface for the front end to create reports

def measure_data_V1(db, company_id, measures, frequency=None, start_date=None, end_date=None,
    groupby=None, measure_filter=None, dimension_filters=None, score_kpis=True):
    
    result = measure_data(db, company_id, measures, frequency, start_date, end_date,
                          groupby, measure_filter, dimension_filters, score_kpis)

    V1_result = result
    if groupby:    
        V1_result = {}
        for code in result:
            print result[code]
            new_data = {}
            for level in result[code]:
                print level
                for date in result[code][level]:
                    print result[code][level][date]
                    new_data[level+date] = result[code][level][date]
            V1_result[code] = new_data
    return V1_result


def measure_data(db, company_id, measures, frequency=None, start_date=None, end_date=None,
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

    The return value will vary on whether groupby is passed or not.

    Without groupby the return will look like this:
    {
        'CODE1':{
            'period1':100.0,
            'period5':100.0,
        },
        'CODE2':{
            'period2':130.0,
            'period3':130.0,
        }
    }

    CODE1 and CODE2 will be the measure codes of the measure IDs passed.
    periods are strings formatted as follows:
    For day frequency: YYYY-MM-DD
    For month frequency: YYYY-MM
    For quarter frequency: YYYY-Q[1-4]


    With a groupby, an additional level is added, like this:
    {
        'CODE1':{
            'LEV1':{
                'period1':100.0,
                'period5':100.0,
            },
            'LEV2':{
                'period1':100.0,
                'period5':100.0,
            },

        }
    }

    Where LEV1 and LEV2 are levels for the dimension specified.

    Periods and levels without data are omitted from the data structure.
    e.g. there is no need to have a '2011-03':None in the returned dictionary



    """
    ks_precompute = precompute(db)
    bigTable = "BigTable"+ str(ks_precompute.getMaxBigTableIdForCompany(company_id))
    
    ks_analytics = analytics(db)
    
    data = ks_analytics.getMeasureData(bigTable, 
                                       measures, 
                                       start_date,
                                       end_date,
                                       groupby, 
                                       dimension_filters,
                                       frequency)
    
    return data    
    
