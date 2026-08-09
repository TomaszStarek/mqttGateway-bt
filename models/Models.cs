using System.Collections.Generic;

namespace MqttModbusGateway
{
    // ---------------------------------------------------------------------------
    // Gateway configuration (received from MQTT config topic)
    // ---------------------------------------------------------------------------

    /// <summary>
    /// Root configuration object deserialized from the MQTT config topic payload.
    /// Contains a list of CEM3-G-BT torque wrench definitions to be managed by the gateway.
    /// </summary>
    public record ConfigRoot
    {
        public long Timestamp { get; set; }
        public bool IsActive { get; set; }
        public List<DeviceConfig> Devices { get; set; } = new();
    }

    public record DeviceConfig
    {
        public int Id { get; set; }
        public string IpAddress { get; set; } = string.Empty;
        public int SmartToolType { get; set; }

        public string DeviceId => $"DEVICE-{Id}";
        public string Address => IpAddress;
        public int BaudRate => 9600;
        public string SerialNumber => string.Empty;
    }

    // ---------------------------------------------------------------------------
    // Fastening event – identical shape to the previous Modbus implementation
    // so that downstream AWS consumers need no changes.
    // Fields are populated from the DF-3 communication frame (manual §6.4).
    // ---------------------------------------------------------------------------

    /// <summary>
    /// Represents a single fastening-cycle result received from the CEM3-G-BT
    /// over Bluetooth SPP in DF-3 format.
    ///
    /// DF-3 frame layout (59 characters + CRLF):
    /// <code>
    /// RE,001,+100.0nm,+090,deg,OO,1234567,18/12/31,12:59:59CRLF
    /// </code>
    /// </summary>
    /// <param name="StepId">
    ///   ID of the job step that was active (via the last <c>configureJob</c> command)
    ///   when this event was produced. 0 if no job has been configured yet.
    /// </param>
    public record FasteningEvent(

        int EventCount,

        float TargetTorqueLowNm,
        float TargetTorqueHighNm,
        float ConvertedTorqueNm,

        float TargetTorqueTriggerNm,

        int DoubleDetectionAngleDeg,
        int TargetAngleLowDeg,
        int TargetAngleHighDeg,
        int TotalAngleDeg,

        bool IsLoosening,


        string DeviceId,
        string Timestamp,
        bool Result,

        int StepId

    );

    /// <summary>
    /// Payload published to <c>{thingName}/{address}/data</c>.
    /// Property declaration order below matches the wire/JSON field order
    /// (System.Text.Json serializes records in declaration order):
    /// deviceId, timestamp, result, isLoosening, targetTorqueLowNm, targetTorqueHighNm,
    /// convertedTorqueNm, totalAngleDeg, targetTorqueTriggerNm, doubleDetectionAngleDeg,
    /// targetAngleLowDeg, targetAngleHighDeg, stepId.
    /// </summary>
    public record ResultsToSend(

        string DeviceId,
        string Timestamp,
        bool Result,
        bool IsLoosening,

        float TargetTorqueLowNm,
        float TargetTorqueHighNm,
        float ConvertedTorqueNm,

        int TotalAngleDeg,

        float TargetTorqueTriggerNm,
        int DoubleDetectionAngleDeg,
        int TargetAngleLowDeg,
        int TargetAngleHighDeg,

        int StepId
    );


    /// <summary>
    /// A command received from the MQTT commands topic
    /// (<c>{thingName}/{address}/commands</c>).
    ///
    /// Two shapes are supported, distinguished by <see cref="Type"/>:
    ///
    /// <para><c>"raw"</c> — sends <see cref="AtCommand"/> verbatim to the wrench.</para>
    ///
    /// <para><c>"configureJob"</c> — payload example:</para>
    /// <code>
    /// {
    ///   "type": "configureJob",
    ///   "targetTorqueNm": 10.0,
    ///   "torqueMinPercentage": 10,
    ///   "torqueMaxPercentage": 10,
    ///   "minAngleDeg": 15,
    ///   "rotationStartThresholdPercentage": 10,
    ///   "stepId": 1
    /// }
    /// </code>
    /// The absolute torque limits are derived by the gateway as:
    /// <c>torqueLowNm = targetTorqueNm * TorqueMinPercentage / 100</c>,
    /// <c>torqueHighNm = targetTorqueNm * TorqueMaxPercentage / 100</c>,
    /// and the trigger torque as
    /// <c>triggerNm = targetTorqueNm * RotationStartThresholdPercentage / 100</c>.
    /// Angle upper limit is not checked and is sent as the protocol maximum;
    /// double-detection angle is not used and is sent as 0.
    /// </summary>
    public class Command
    {
        public string? Type { get; set; }

        // raw AT command
        public string? AtCommand { get; set; }
        public float TargetTorqueNm { get; set; }
        public float TorqueMinPercentage { get; set; }
        public float TorqueMaxPercentage { get; set; }
        public int MinAngleDeg { get; set; }
        public float RotationStartThresholdPercentage { get; set; }
        public int StepId { get; set; }
    }
}