# CI/CD and developing airflow code best practices

## Start small
Anytime we're developing new code that processes data, it's best to start on a small sample.  This saves time and allows us to iterate faster.  In this case, I set the start and end dates in the dag to only be for the first few days of the data to test.

One thing I noticed is if a ton of tasks get spun up, they can sometimes continue to run for a while.  This can cause worker nodes to run out of memory, which makes any new task runs fail with no logs or output.

It seems to also cause log jams.

## Log jams
I found that airflow seems to have quirks and maybe bugs.  It seems like logs and DAG/task runs can get stuck if too many are run, so that you can re-run a DAG after updating custom plugin code that get used, but the logs don't seem to update.  I found that you can clear both the DAG runs and task history like so:
(sparkify_etl is the name of the DAG here)

`airflow clear sparkify_etl`
`airflow delete_dag sparkify_etl`

Note: it seems like deleting the DAG is enough -- if you first delete the DAG, then there's usually no tasks.

In puckel's docker container method, this is:

`docker-compose -f docker-compose-CeleryExecutor.yml run --rm webserver airflow clear -c sparkify_etl`

`docker-compose -f docker-compose-CeleryExecutor.yml run --rm webserver airflow delete_dag -y sparkify_etl`

The corresponding commands on Google Cloud Composer (from their cloud shell):

`gcloud composer environments run udacity-p4 --location us-central1 clear -- -c sparkify_etl`

`gcloud composer environments run udacity-p4 --location us-central1 delete_dag -- -y sparkify_etl`

udacity-p4 is the instance name here and sparkify_etl is the DAG name.  The -c is an option for clear to not ask for confirmation (otherwise it hangs).  The location is a mandatory argument.

Note these take a bit of time to run on GCP.

You can also clean tasks and DAG run history manually (yuck) through the GUI with:

- `Browse->Task Instances->Select` all and `With Selected->Clear`
- Click on DAG runs (circles), remove filter by failed/succeeded/etc, `With Selected->Delete`

## Plugin problems
For some reason plugin changes are not always detected quickly.  I would delete all task and DAG data, then sometimes even reboot the docker containers to fix this.

`docker-compose -f docker-compose-CeleryExecutor.yml restart`

Or use `down` and `up` if `restart` doesn't do it.

## GCP setup
GCP defaults to `dags_are_paused_at_creation = True` in the airflow.cfg file.  To change it, go to 'airflow configuration overrides' and add a config:

```
section: core
key: dags_are_paused_at_creation
value: True
```

This takes about 2 or 3 minutes to update.

Google also has some tips for testing and developing DAGs: https://cloud.google.com/composer/docs/how-to/using/testing-dags
