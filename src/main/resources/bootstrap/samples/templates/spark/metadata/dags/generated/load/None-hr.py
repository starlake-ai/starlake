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
  'schedule': 'None',
  'cron': None,
  'domains': [
    {
      'name':'hr',
      'tables': [
          {
              'name': 'sellers',
              'final_name': 'sellers'
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

