description='sample dag configuration'
template='sample.py.j2'


options = {
    'jarFileUrisVar':'SL_JARS', 
    'profileVar':'DATAPROC_MEDIUM', 
    'envVar':'SL_ENV', 
    'SL_TIMEZONE':'Europe/Paris', 
    'region':'europe-west1'
    
}


schedules= [
{
  'schedule': '0 0 * * *',
  'cron': '0 0 * * *',
  'domains': [
    {
      'name':'hr',
      'tables': [
          {
              'name': 'locations',
              'final_name': 'locations'
          }
      ],
    }
    
  ]
}

]

# dataproc[schedule, domain, table]
# spark-serverless[schedule, domain, table]
# cloudrun[schedule, domain, table]
## databricks[schedule, domain, table]

