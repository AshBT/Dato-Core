.. _deploy:

##########
Deployment
##########

.. automodule:: graphlab.deploy

.. currentmodule:: graphlab.deploy


Distributed/Asynchronous Execution
----------------------------------
.. autosummary::
  :toctree: generated/

  job.create
  map_job.create
  Job

Predictive Services
-------------------
.. autosummary::
  :toctree: generated/

  predictive_service.create
  predictive_service.load

Environments
------------
.. autosummary::
  :toctree: generated/

  environment.Local
  environment.LocalAsync
  environment.EC2
  environment.Hadoop

Session Management
------------------
.. autosummary::
  :toctree: generated/

  environments
  jobs
  predictive_services


.. autosummary::
  :toctree: generated/
  :hiddeninclude:
  _predictive_service._predictive_service.PredictiveService
  _predictive_service._predictive_object.PredictiveObject
  _predictive_service._model_predictive_object.ModelPredictiveObject
  _predictive_service._predictive_object.ModelPredictiveObject
  _session.ScopedSession
