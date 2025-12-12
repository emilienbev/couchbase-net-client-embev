using System.Text.Json.Serialization;
using Couchbase;
using Couchbase.Core.Exceptions.KeyValue;
using Couchbase.KeyValue;
using Couchbase.KeyValue.RangeScan;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

var builder = WebApplication.CreateBuilder(args);

// Configure Kestrel to listen on port 9999
builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenLocalhost(9999);
});

// Configure Couchbase connection
var cluster = await Cluster.ConnectAsync(
    connectionString: "couchbase://localhost:12000",
    options =>
    {
        options.UserName = "Administrator";
        options.Password = "asdasd";
    }).ConfigureAwait(false);

var bucket = await cluster.BucketAsync("default").ConfigureAwait(false);
var scope = await bucket.ScopeAsync("_default").ConfigureAwait(false);
var collection = await scope.CollectionAsync("_default").ConfigureAwait(false);

// Store cluster and collection as singletons for use in endpoints
builder.Services.AddSingleton(cluster);
builder.Services.AddSingleton(collection);

// Add CORS for React frontend
builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy =>
    {
        policy.AllowAnyOrigin()
              .AllowAnyMethod()
              .AllowAnyHeader();
    });
});

var app = builder.Build();

app.UseCors();

// Health check endpoint
app.MapGet("/health", async (ICluster cluster) =>
{
    try
    {
        var healthStatus = new
        {
            Status = "healthy",
            Timestamp = DateTime.UtcNow
        };

        return Results.Ok(healthStatus);
    }
    catch (Exception ex)
    {
        var errorStatus = new
        {
            Status = "unhealthy",
            Timestamp = DateTime.UtcNow,
            Error = ex.Message,
            Couchbase = new
            {
                Connected = false
            }
        };

        return Results.Ok(errorStatus);
    }
});

// Upsert endpoint - Insert or update a document
app.MapPost("/api/documents/{id}", async (string id, HttpRequest request, ICouchbaseCollection collection) =>
{
    Console.WriteLine($"[UPSERT] Starting upsert for document ID: {id}");

    try
    {
        // Read the raw request body
        using var reader = new StreamReader(request.Body);
        var jsonContent = await reader.ReadToEndAsync().ConfigureAwait(false);

        Console.WriteLine($"[UPSERT] Received content length: {jsonContent?.Length ?? 0}");
        Console.WriteLine($"[UPSERT] Content: {jsonContent}");

        if (string.IsNullOrWhiteSpace(jsonContent))
        {
            Console.WriteLine("[UPSERT] ERROR: Content is empty");
            return Results.BadRequest(new
            {
                Success = false,
                Error = "Request body cannot be empty"
            });
        }

        // Parse the JSON string into an object to validate and store
        Console.WriteLine("[UPSERT] Attempting to deserialize JSON...");
        var document = JsonConvert.DeserializeObject<dynamic>(jsonContent);

        if (document == null)
        {
            Console.WriteLine("[UPSERT] ERROR: Deserialized document is null");
            return Results.BadRequest(new
            {
                Success = false,
                Error = "Invalid JSON format"
            });
        }

        Console.WriteLine($"[UPSERT] Deserialized successfully. Document type: {document.GetType().Name}");

        // Store the object in Couchbase
        Console.WriteLine($"[UPSERT] Calling Couchbase UpsertAsync for ID: {id}");
        var result = await collection.UpsertAsync(id, document).ConfigureAwait(false);

        Console.WriteLine($"[UPSERT] SUCCESS!");

        return Results.Ok(new
        {
            Success = true,
            Id = id
        });
    }
    catch (JsonException jsonEx)
    {
        Console.WriteLine($"[UPSERT] JSON EXCEPTION: {jsonEx.Message}");
        Console.WriteLine($"[UPSERT] Stack trace: {jsonEx.StackTrace}");
        return Results.BadRequest(new
        {
            Success = false,
            Error = "Invalid JSON: " + jsonEx.Message
        });
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[UPSERT] EXCEPTION: {ex.GetType().Name}");
        Console.WriteLine($"[UPSERT] Message: {ex.Message}");
        Console.WriteLine($"[UPSERT] Stack trace: {ex.StackTrace}");
        return Results.BadRequest(new
        {
            Success = false,
            Error = ex.Message,
            ExceptionType = ex.GetType().Name
        });
    }
});

// Get endpoint - Retrieve a document
app.MapGet("/api/documents/{id}", async (string id) =>
{
    try
    {
        var result = await collection.GetAsync(id).ConfigureAwait(false);
        var content = result.ContentAs<JObject>()?.ToString();

        return Results.Ok(new
        {
            Success = true,
            Id = id,
            Cas = result.Cas.ToString(), // Convert to string for safe JavaScript handling
            Content = content
        });
    }
    catch (DocumentNotFoundException)
    {
        return Results.NotFound(new
        {
            Success = false,
            Error = $"Document with id '{id}' not found"
        });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new
        {
            Success = false,
            Error = ex.Message,
            ExceptionType = ex.GetType().Name
        });
    }
});

// GetBefore endpoint - Get a previous version of a document based on CAS
app.MapGet("/api/documents/{id}/before", async (string id, string cas, ICouchbaseCollection collection) =>
{
    try
    {
        // Parse the CAS string to ulong
        if (!ulong.TryParse(cas, out var casValue))
        {
            return Results.BadRequest(new
            {
                Success = false,
                Error = $"Invalid CAS value: {cas}"
            });
        }

        var result = await collection.GetBeforeAsync(id, casValue).ConfigureAwait(false);
        var content = result.ContentAs<JObject>()?.ToString();

        return Results.Ok(new
        {
            Success = true,
            Id = id,
            Cas = result.Cas.ToString(), // Convert to string for safe JavaScript handling
            Content = content,
            Message = $"Retrieved version before CAS {cas}"
        });
    }
    catch (DocumentNotFoundException)
    {
        return Results.NotFound(new
        {
            Success = false,
            Error = $"No previous version found for document '{id}' before CAS {cas}"
        });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new
        {
            Success = false,
            Error = ex.Message,
            ExceptionType = ex.GetType().Name
        });
    }
});

// Get Tombstone
app.MapGet("/api/documents/{id}/tombstone", async (string id, ICouchbaseCollection collection) =>
{
    try
    {
        var result = await collection.GetBeforeAsync(id, ulong.MaxValue).ConfigureAwait(false);
        var content = result.ContentAs<JObject>()?.ToString();

        if (!string.IsNullOrEmpty(content))
        {
            return Results.NotFound(new
            {
                Success = false,
                Error = $"Document '{id}' is not deleted (no tombstone found)"
            });
        }

        return Results.Ok(new
        {
            Success = true,
            Id = id,
            Cas = result.Cas.ToString(),
            Content = content,
            Message = "Retrieved tombstone"
        });
    }
    catch (DocumentNotFoundException)
    {
        return Results.NotFound(new
        {
            Success = false,
            Error = $"No previous version found for document"
        });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new
        {
            Success = false,
            Error = ex.Message,
            ExceptionType = ex.GetType().Name
        });
    }
});

// Get all documents endpoint - Scans entire collection
app.MapGet("/api/documents/all", async (ICouchbaseCollection collection) =>
{
    Console.WriteLine("[SCAN ALL] Starting scan of all documents...");

    try
    {
        var rangeScan = new PrefixScan(""); // Empty prefix catches all documents
        var scanResult = collection.ScanAsync(rangeScan);

        var result = new List<object>();
        var count = 0;

        await foreach (var item in scanResult.ConfigureAwait(false))
        {
            count++;
            Console.WriteLine($"[SCAN ALL] Found document #{count}: ID={item.Id}, CAS={item.Cas}");

            try
            {
                var content = item.ContentAs<JObject>()?.ToString();
                result.Add(new
                {
                    Success = true,
                    Id = item.Id,
                    Cas = item.Cas.ToString(), // Convert to string for safe JavaScript handling
                    Content = content
                });
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[SCAN ALL] Error processing document {item.Id}: {ex.Message}");
                // Still add the document but with error info
                result.Add(new
                {
                    Success = false,
                    Id = item.Id,
                    Cas = item.Cas.ToString(),
                    Error = ex.Message
                });
            }
        }

        Console.WriteLine($"[SCAN ALL] Scan complete! Found {count} document(s)");

        return Results.Ok(new
        {
            Success = true,
            Count = count,
            Documents = result
        });
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[SCAN ALL] EXCEPTION: {ex.GetType().Name}");
        Console.WriteLine($"[SCAN ALL] Message: {ex.Message}");
        Console.WriteLine($"[SCAN ALL] Stack trace: {ex.StackTrace}");

        return Results.BadRequest(new
        {
            Success = false,
            Error = ex.Message,
            ExceptionType = ex.GetType().Name
        });
    }
});

// Root endpoint
app.MapGet("/", () => Results.Ok(new
{
    Service = "Hackathon Backend",
    Version = "1.0.0",
    Endpoints = new[]
    {
        "GET  /health - Health check endpoint",
        "POST /api/documents/{id} - Upsert a document",
        "GET  /api/documents/{id} - Get a document",
        "GET  /api/documents/{id}/tombstone - Get a tombstone (deleted document)",
        "GET  /api/documents/{id}/before?cas={cas} - Get previous version of a document",
        "GET  /api/documents/all - Get all documents in the collection"
    }
}));





app.Run();
