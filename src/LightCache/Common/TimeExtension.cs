using System;

namespace LightCache.Common
{
    public static class TimeExtension
    {
        public static TimeSpan? ToTimeSpan(this DateTimeOffset? offset, bool utc = false)
        {
            if (offset == null)
                return null;

            if (utc)
                return offset.Value.UtcDateTime.Subtract(DateTime.UtcNow);
            return offset.Value.DateTime.Subtract(DateTime.Now);
        }

        public static TimeSpan ToTimeSpan(this DateTimeOffset offset, bool utc = false)
        {
            if (utc)
                return offset.UtcDateTime.Subtract(DateTime.UtcNow);
            return offset.DateTime.Subtract(DateTime.Now);
        }
    }
}
