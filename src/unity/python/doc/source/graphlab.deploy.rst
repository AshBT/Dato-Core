.. _deploy:

##########
Deployment
##########

.. automodule:: graphlab.deploy

.. currentmodule:: graphlab.deploy

Tasks
-----
.. autosummary::
  :toctree: generated/

  Task
  tasks

Jobs
----
.. autosummary::
  :toctree: generated/

  job.create
  jobs
  parallel_for_each

Predictive Services
-------------------
.. autosummary::
  :toctree: generated/

  predictive_service.create
  predictive_service.load
  predictive_services

Environments
------------
.. autosummary::
  :toctree: generated/

  environment.Local
  environment.LocalAsync
  environment.EC2
  environment.Hadoop
  environments

.. autosummary::
  :toctree: generated/
  :hiddeninclude:
  _job.Job
  _predictive_service._predictive_service.PredictiveService
  _predictive_service._predictive_object.PredictiveObject
  _predictive_service._model_predictive_object.ModelPredictiveObjec
  _predictive_service._predictive_object.ModelPredictiveObject
  _session.ScopedSession
