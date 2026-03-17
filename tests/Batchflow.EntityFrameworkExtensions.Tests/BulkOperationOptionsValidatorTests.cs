using Batchflow.EntityFrameworkExtensions.Abstractions;

namespace Batchflow.EntityFrameworkExtensions.Tests;

public class BulkOperationOptionsValidatorTests
{
    [Fact]
    public void Validate_Throws_When_KeyProperties_AreMissing()
    {
        var options = new BulkOperationOptions();

        var exception = Assert.Throws<ArgumentException>(() => BulkOperationOptionsValidator.Validate(options));

        Assert.Contains("key property", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Validate_Throws_When_BatchSize_Is_Not_Positive()
    {
        var options = new BulkOperationOptions
        {
            BatchSize = 0
        };

        options.KeyProperties.Add("ImportKey");

        Assert.Throws<ArgumentOutOfRangeException>(() => BulkOperationOptionsValidator.Validate(options));
    }

    [Fact]
    public void Validate_Does_Not_Throw_For_Minimal_Valid_Options()
    {
        var options = new BulkOperationOptions();
        options.KeyProperties.Add("ImportKey");

        BulkOperationOptionsValidator.Validate(options);
    }
}
