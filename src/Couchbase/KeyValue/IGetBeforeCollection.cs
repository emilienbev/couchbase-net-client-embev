#nullable enable
using System.Threading.Tasks;

namespace Couchbase.KeyValue;

public interface IGetBeforeCollection
{
    /// <summary>
    /// Gets a previous version of a document based on its CAS value.
    /// Document mutations are stored on the server until compaction, and can be retrieved
    /// by specifying a CAS value from a previous version of the document.
    /// Throws DOC_NOT_FOUND if there are no more previous versions in the history of the document.
    /// </summary>
    /// <param name="id">The id of the document.</param>
    /// <param name="cas">The CAS to tell the KV service to get a version previous to</param>
    /// <param name="options">Optional parameters.</param>
    /// <returns>An asynchronous <see cref="Task"/> containing the JSON object or scalar encapsulated in an <see cref="IGetResult"></see> API object.</returns>
    Task<IGetResult> GetBeforeAsync(string id, ulong cas, GetOptions? options = null);
}
