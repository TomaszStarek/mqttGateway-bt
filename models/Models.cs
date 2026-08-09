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
    /// ^  ^   ^        ^    ^   ^  ^       ^         ^
    /// |  |   |        |    |   |  |       |         Time (hh:mm:ss)
    /// |  |   |        |    |   |  |       Date (yy/mm/dd)
    /// |  |   |        |    |   |  7-digit ID (default = serial number)
    /// |  |   |        |    |   Judgment result (2 chars, e.g. OO / LD / HH)
    /// |  |   |        |    Angle unit ("deg")
    /// |  |   |        Angle value with sign (+/-)
    /// |  |   Torque with decimal point and unit ("nm" / "kgf" / "lbf")
    /// |  3-digit memory counter
    /// Header "RE"
    /// </code>
    ///
    /// Fields that have no direct DF-3 equivalent (FasteningTimeMs, PresetNo, etc.)
    /// are carried over from the previous Modbus model and kept at their zero/default
    /// values to preserve the MQTT topic schema expected by AWS consumers.
    /// </summary>
    /// <param name="EventCount">
    ///   Monotonically-increasing memory counter from the wrench (001–999).
    /// </param>
    /// <param name="FasteningTimeMs">Not transmitted in DF-3; always 0.</param>
    /// <param name="PresetNo">Not transmitted in DF-3; always 0.</param>
    /// <param name="TargetTorque">Not transmitted in DF-3; always 0.</param>
    /// <param name="ConvertedTorque">
    ///   Measured peak torque in N·m (absolute value of the signed DF-3 field).
    /// </param>
    /// <param name="TargetSpeedRpm">Not transmitted in DF-3; always 0.</param>
    /// <param name="A1Deg">Not transmitted in DF-3; always 0.</param>
    /// <param name="A2Deg">Not transmitted in DF-3; always 0.</param>
    /// <param name="A3Deg">Final angle in degrees (from DF-3 angle field).</param>
    /// <param name="ScrewCountValue">Same as <see cref="EventCount"/>.</param>
    /// <param name="Error">
    ///   Numeric error code derived from the 2-char judgment string:
    ///   0 = no error / OO judgment, non-zero = NG (see <see cref="JudgmentCode"/>).
    /// </param>
    /// <param name="IsLoosening">
    ///   <c>true</c> when the torque sign in the DF-3 frame is negative (CCW operation).
    /// </param>
    /// <param name="Status">
    ///   Raw judgment bitmask: bit 0 = torque OK, bit 1 = angle OK.
    ///   0b11 = full pass; lower values indicate NG conditions.
    /// </param>
    /// <param name="SnugTorqueAngle">Not transmitted in DF-3; always 0.</param>
    /// <param name="Timestamp">UTC timestamp (ISO 8601) of when the frame was received.</param>
    /// <param name="JudgmentCode">
    ///   Raw 2-char judgment string from the wrench, e.g. "OO", "LD", "HH".
    ///   First char = torque judgment (O/L/H/D/N), second = angle judgment (O/L/H/D/–).
    /// </param>
    /// <param name="TorqueUnit">
    ///   Torque unit as reported by the wrench ("nm", "kgf", "lbf", etc.).
    /// </param>
    /// <param name="DeviceId">
    ///   Gateway-assigned identifier of the wrench that produced this event.
    /// </param>
    public record FasteningEvent(

        int   EventCount,

        float TargetTorqueLowNm,
        float TargetTorqueHighNm,
        float  ConvertedTorqueNm,

        float TargetTorqueTriggerNm,

        int DoubleDetectionAngleDeg,
        int TargetAngleLowDeg,
        int TargetAngleHighDeg,
        int TotalAngleDeg,

        bool IsLoosening,


        string DeviceId,
        string Timestamp,
        bool Result,
        int? StepId

    );

    public record ResultsToSend(

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
        int? StepId
    );


    /// <summary>
    /// A write command received from the MQTT commands topic.
    /// Instructs the gateway to send an AT-command to the target wrench over SPP.
    /// </summary>
    /// <param name="AtCommand">
    ///   Full AT command string without the trailing CRLF,
    ///   e.g. <c>"AT037,45.00,30.00"</c> (sets Hi/Lo torque limits).
    ///   See manual §6.5 for the complete command list.
    /// </param>
    public class Command
    {
        public string? Type { get; set; }

        // raw AT command
        public string? AtCommand { get; set; }

        // configureJob
        public float TargetTorqueNm { get; set; }
        public float TorqueMinPercentage { get; set; }
        public float TorqueMaxPercentage { get; set; }
        public int MinAngleDeg { get; set; }
        public float RotationStartThresholdPercentage { get; set; }
        public int StepId { get; set; }
    }
}
