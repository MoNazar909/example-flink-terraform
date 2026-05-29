-- Step 1: Flatten EOI source messages into common topic
-- Reads from:  standard-eda-bentech-eoi
-- Success  →   standard-eda-bentechdata-flink-common
-- Messages where eventMetaData or eventData is null are dropped (no DLQ for source topic)
--
-- flattenedEvent is a JSON string in Avro JSON encoding format containing the full source message.
-- Arrays are accessed at index [1] — assumes one employer, one employee, one coverage per message.

INSERT INTO `standard-eda-bentechdata-flink-common`
(kafka_key, flattenedEvent, exception, targetBentech, groupId, eventType, correlationId)
SELECT
  CONCAT(eventMetaData.groupId, '-', eventMetaData.workerId, '-', eventMetaData.targetBentech, '-', eventMetaData.eventType),
  JSON_OBJECT(
    KEY 'eventMetaData' VALUE JSON_OBJECT(
      KEY 'com.standardinsurance.eoi.EventMetaData' VALUE JSON_OBJECT(
        KEY 'messageSource'       VALUE JSON_OBJECT(KEY 'string' VALUE eventMetaData.messageSource),
        KEY 'messageVersion'      VALUE JSON_OBJECT(KEY 'string' VALUE eventMetaData.messageVersion),
        KEY 'correlationId'       VALUE JSON_OBJECT(KEY 'string' VALUE eventMetaData.correlationId),
        KEY 'groupId'             VALUE JSON_OBJECT(KEY 'string' VALUE eventMetaData.groupId),
        KEY 'targetBentech'       VALUE JSON_OBJECT(KEY 'string' VALUE eventMetaData.targetBentech),
        KEY 'targetHostname'      VALUE JSON_OBJECT(KEY 'string' VALUE eventMetaData.targetHostname),
        KEY 'targetTokenHostname' VALUE JSON_OBJECT(KEY 'string' VALUE eventMetaData.targetTokenHostname),
        KEY 'eventType'           VALUE JSON_OBJECT(KEY 'string' VALUE eventMetaData.eventType),
        KEY 'contentType'         VALUE JSON_OBJECT(KEY 'string' VALUE eventMetaData.contentType),
        KEY 'workerId'            VALUE JSON_OBJECT(KEY 'string' VALUE eventMetaData.workerId)
      )
    ),
    KEY 'eventData' VALUE JSON_OBJECT(
      KEY 'com.standardinsurance.eoi.EventData' VALUE JSON_OBJECT(
        KEY 'transmission' VALUE JSON_OBJECT(
          KEY 'com.standardinsurance.eoi.Transmission' VALUE JSON_OBJECT(
            KEY 'transmissionGuid'     VALUE JSON_OBJECT(KEY 'string' VALUE eventData.transmission.transmissionGuid),
            KEY 'senderName'           VALUE JSON_OBJECT(KEY 'string' VALUE eventData.transmission.senderName),
            KEY 'senderPlatformName'   VALUE JSON_OBJECT(KEY 'string' VALUE eventData.transmission.senderPlatformName),
            KEY 'receiverName'         VALUE JSON_OBJECT(KEY 'string' VALUE eventData.transmission.receiverName),
            KEY 'creationDateTime'     VALUE JSON_OBJECT(KEY 'string' VALUE eventData.transmission.creationDateTime),
            KEY 'testProductionCode'   VALUE JSON_OBJECT(KEY 'string' VALUE eventData.transmission.testProductionCode),
            KEY 'transmissionTypeCode' VALUE JSON_OBJECT(KEY 'string' VALUE eventData.transmission.transmissionTypeCode),
            KEY 'employer' VALUE JSON_OBJECT(KEY 'array' VALUE JSON_ARRAY(
              JSON_OBJECT(
                KEY 'employerPartyId'       VALUE JSON_OBJECT(KEY 'string' VALUE eventData.transmission.employer[1].employerPartyId),
                KEY 'masterAgreementNumber' VALUE JSON_OBJECT(KEY 'string' VALUE eventData.transmission.employer[1].masterAgreementNumber),
                KEY 'employerName'          VALUE JSON_OBJECT(KEY 'string' VALUE eventData.transmission.employer[1].employerName),
                KEY 'employee' VALUE JSON_OBJECT(KEY 'array' VALUE JSON_ARRAY(
                  JSON_OBJECT(
                    KEY 'employeePartyId'              VALUE JSON_OBJECT(KEY 'string' VALUE eventData.transmission.employer[1].employee[1].employeePartyId),
                    KEY 'employeeSocialSecurityNumber' VALUE JSON_OBJECT(KEY 'string' VALUE eventData.transmission.employer[1].employee[1].employeeSocialSecurityNumber),
                    KEY 'employeeIdentifier'           VALUE JSON_OBJECT(KEY 'string' VALUE eventData.transmission.employer[1].employee[1].employeeIdentifier),
                    KEY 'employeeName' VALUE JSON_OBJECT(
                      KEY 'com.standardinsurance.eoi.EmployeeName' VALUE JSON_OBJECT(
                        KEY 'firstName' VALUE JSON_OBJECT(KEY 'string' VALUE eventData.transmission.employer[1].employee[1].employeeName.firstName),
                        KEY 'lastName'  VALUE JSON_OBJECT(KEY 'string' VALUE eventData.transmission.employer[1].employee[1].employeeName.lastName)
                      )
                    ),
                    KEY 'coverage' VALUE JSON_OBJECT(KEY 'array' VALUE JSON_ARRAY(
                      JSON_OBJECT(
                        KEY 'coverageId'            VALUE JSON_OBJECT(KEY 'string' VALUE eventData.transmission.employer[1].employee[1].coverage[1].coverageId),
                        KEY 'groupPolicyNumber'     VALUE JSON_OBJECT(KEY 'string' VALUE eventData.transmission.employer[1].employee[1].coverage[1].groupPolicyNumber),
                        KEY 'productTypeCode'       VALUE JSON_OBJECT(KEY 'string' VALUE eventData.transmission.employer[1].employee[1].coverage[1].productTypeCode),
                        KEY 'benefitPlanIdentifier' VALUE JSON_OBJECT(KEY 'string' VALUE eventData.transmission.employer[1].employee[1].coverage[1].benefitPlanIdentifier),
                        KEY 'coverageInsured' VALUE JSON_OBJECT(KEY 'array' VALUE JSON_ARRAY(
                          JSON_OBJECT(
                            KEY 'insuredPartyId' VALUE JSON_OBJECT(KEY 'string' VALUE eventData.transmission.employer[1].employee[1].coverage[1].coverageInsured[1].insuredPartyId)
                          )
                        )),
                        KEY 'underwritingStatus' VALUE JSON_OBJECT(
                          KEY 'com.standardinsurance.eoi.UnderwritingStatus' VALUE JSON_OBJECT(
                            KEY 'underwritingDecisionCode'          VALUE JSON_OBJECT(KEY 'string' VALUE eventData.transmission.employer[1].employee[1].coverage[1].underwritingStatus.underwritingDecisionCode),
                            KEY 'underwritingDecisionEffectiveDate' VALUE JSON_OBJECT(KEY 'string' VALUE eventData.transmission.employer[1].employee[1].coverage[1].underwritingStatus.underwritingDecisionEffectiveDate),
                            KEY 'underwritingApprovedBenefitAmount' VALUE JSON_OBJECT(KEY 'string' VALUE eventData.transmission.employer[1].employee[1].coverage[1].underwritingStatus.underwritingApprovedBenefitAmount),
                            KEY 'totalBenefitAmount'                VALUE JSON_OBJECT(KEY 'string' VALUE eventData.transmission.employer[1].employee[1].coverage[1].underwritingStatus.totalBenefitAmount),
                            KEY 'applicationStatusCode'             VALUE JSON_OBJECT(KEY 'string' VALUE eventData.transmission.employer[1].employee[1].coverage[1].underwritingStatus.applicationStatusCode),
                            KEY 'applicationStatusDateTime'         VALUE JSON_OBJECT(KEY 'string' VALUE eventData.transmission.employer[1].employee[1].coverage[1].underwritingStatus.applicationStatusDateTime)
                          )
                        )
                      )
                    ))
                  )
                ))
              )
            )),
            KEY 'audit' VALUE JSON_OBJECT(
              KEY 'com.standardinsurance.eoi.Audit' VALUE JSON_OBJECT(
                KEY 'auditId'                          VALUE JSON_OBJECT(KEY 'string' VALUE eventData.transmission.audit.auditId),
                KEY 'employerRecordQuantity'           VALUE JSON_OBJECT(KEY 'int'    VALUE eventData.transmission.audit.employerRecordQuantity),
                KEY 'employeeRecordQuantity'           VALUE JSON_OBJECT(KEY 'int'    VALUE eventData.transmission.audit.employeeRecordQuantity),
                KEY 'dependentRecordQuantity'          VALUE JSON_OBJECT(KEY 'int'    VALUE eventData.transmission.audit.dependentRecordQuantity),
                KEY 'coverageRecordQuantity'           VALUE JSON_OBJECT(KEY 'int'    VALUE eventData.transmission.audit.coverageRecordQuantity),
                KEY 'underwritingStatusRecordQuantity' VALUE JSON_OBJECT(KEY 'int'    VALUE eventData.transmission.audit.underwritingStatusRecordQuantity)
              )
            )
          )
        )
      )
    )
  ),
  CAST(NULL AS STRING),
  eventMetaData.targetBentech,
  eventMetaData.groupId,
  eventMetaData.eventType,
  eventMetaData.correlationId
FROM `standard-eda-bentech-eoi`
WHERE eventMetaData IS NOT NULL AND eventData IS NOT NULL;
