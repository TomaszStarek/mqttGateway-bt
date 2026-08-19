using System;
using System.Collections.Generic;

namespace MqttModbusGateway
{
    public record ConfigRoot
    {
        public long Timestamp { get; set; }
        public bool IsActive { get; set; }
        public List<DeviceConfig> Devices { get; set; } = new();
    }

    public record DeviceDto(
        int Id,
        string IpAddress,
        int SmartToolType
    );

    public record DeviceConfig(
        int Id,
        string IpAddress,
        int SmartToolType,
        int BaudRate = 9600,
        string? SerialNumber = null
    )
    {
        public string Address => IpAddress;
        public string DeviceId => $"DEVICE-{Id}";
    }

    public record StepCommand(
        int StepId,
        int BatchId,
        int DeviceId,
        int UserId,
        double TargetTorqueNm,
        double TorqueMinPercentage,
        double TorqueMaxPercentage,
        int MinAngleDeg,
        int RotationStartThresholdPercentage
    );

    public record RawCommand(
        string Type,
        string AtCommand
    );

    public record DeviceState(
        bool Connected,
        string? DisconnectReason,
        string Timestamp
    );

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
        int DeviceId,
        string Timestamp,
        bool Result,
        int StepId,
        int BatchId,
        int UserId
    );

    /// <summary>
    /// Payload matching the TorqueLog structure for downstream services.
    /// </summary>
    public record ResultsToSend(
        float TargetTorqueLowNm,
        float TargetTorqueHighNm,
        float ConvertedTorqueNm,

        int TargetSpeedRpm,
        int FasteningTimeMs,

        int A1Deg,
        int A2Deg,

        int TotalAngleDeg,

        bool IsLoosening,

        int SnugTorqueAngle,

        bool Result,
        int StepId,
        int DeviceId,
        int BatchId,
        int UserId

    );

    public class Command
    {
        public string? Type { get; set; }
        public string? AtCommand { get; set; }
        public float TargetTorqueNm { get; set; }
        public float TorqueMinPercentage { get; set; }
        public float TorqueMaxPercentage { get; set; }
        public int MinAngleDeg { get; set; }
        public float RotationStartThresholdPercentage { get; set; }
        public int StepId { get; set; }
        public int BatchId { get; set; }
        public int UserId { get; set; }
        public int DeviceId { get; set; }
    }
}