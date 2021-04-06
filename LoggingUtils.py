import logging

class logging_utils():
 def __init__(self,JOB_NAME,JOB_ID):
  self.logger = logging.getLogger()
  log_format = f"%(levelname)s RequestId:<{JOB_ID}>  {JOB_NAME}: %(lineno)d - %(message)s"
  logging.getLogger("elasticsearch").setLevel(logging.WARNING)
  logging.getLogger("requests").setLevel(logging.WARNING)
  logging.getLogger("awswrangler").setLevel(logging.WARNING)
  logging.getLogger("botocore").setLevel(logging.WARNING)
  if self.logger.handlers:
   for handler in self.logger.handlers:
    self.logger.removeHandler(handler)
  logging.basicConfig(level=logging.INFO, format=log_format)


 def logging(self):
  return self.logger
