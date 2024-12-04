using Tests.Infrastructure.ConnectionString;
using Xunit;

namespace Tests.Infrastructure;

public class RequiresSnowflakeFactAttribute : FactAttribute
{
    public RequiresSnowflakeFactAttribute()
    {
        if (RavenTestHelper.SkipIntegrationTests)
        {
            Skip = RavenTestHelper.SkipIntegrationMessage;
            return;
        }

        if (RavenTestHelper.IsRunningOnCI)
            return;

        if (SnowflakeConnectionString.Instance.CanConnect == false)
            Skip = "Test requires Snowflake database";
    }
    
    internal static bool ShouldSkip(out string skipMessage)
    {
        if (RavenTestHelper.SkipIntegrationTests)
        {
            skipMessage = RavenTestHelper.SkipIntegrationMessage;
            return true;
        }

        if (RavenTestHelper.IsRunningOnCI)
        {
            skipMessage = null;
            return false;
        }

        if (MsSqlConnectionString.Instance.CanConnect)
        {
            skipMessage = null;
            return false;
        }

        skipMessage = "Test requires Snowflake database";
        return true;

    }
}
