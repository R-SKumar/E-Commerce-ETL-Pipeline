{
  "Comment": "ETL pipeline: Trigger Glue and wait for completion, then notify via SNS",
  "StartAt": "StartJobRun",
  "States": {
    "StartJobRun": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:glue:startJobRun",
      "Parameters": {
        "JobName": "GlueJobName",
        "Arguments": {
          "--orders_s3_key.$": "$.orders_s3_key",
          "--returns_s3_key.$": "$.returns_s3_key",
          "--JOB_NAME": "GlueJobName"
        }
      },
      "ResultPath": "$.glueResult",
      "Next": "WaitBeforeCheck"
    },
    "WaitBeforeCheck": {
      "Type": "Wait",
      "Seconds": 20,
      "Next": "GetJobRun"
    },
    "GetJobRun": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:glue:getJobRun",
      "Parameters": {
        "JobName": "GlueJobName",
        "RunId.$": "$.glueResult.JobRunId",
        "PredecessorsIncluded": true
      },
      "ResultPath": "$.JobRun",
      "Next": "Job_Complete?"
    },
    "Job_Complete?": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.JobRun.JobRun.JobRunState",
          "StringEquals": "SUCCEEDED",
          "Next": "SNS_Publish_Success"
        },
        {
          "Variable": "$.JobRun.JobRun.JobRunState",
          "StringEquals": "FAILED",
          "Next": "SNS_Publish_Failure"
        }
      ],
      "Default": "WaitBeforeCheck"
    },
    "SNS_Publish_Success": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "<arn Success Topic>",
        "Message.$": "States.Format('✅ Glue job succeeded! Orders File: {}, Returns File: {}', $.orders_s3_key, $.returns_s3_key)",
        "Subject": "Glue Job Success Notification"
      },
      "End": true
    },
    "SNS_Publish_Failure": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "<arn Failure Topic>",
        "Message": "❌ ETL pipeline failed during Glue job execution.",
        "Subject": "Glue Job Failure Notification"
      },
      "End": true
    }
  }
}