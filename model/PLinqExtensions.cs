using System;
using System.Linq;
using System.Threading.Tasks;

namespace SchemaZen.Library {
	public static class PLinqExtensions {
		public static void BetterForAll<TSource>(this ParallelQuery<TSource> source, Action<TSource> action) {
			try {
				source.ForAll(action);
			} catch (AggregateException ex) {
				var innerException = ex.InnerExceptions.FirstOrDefault();
				
				if (innerException != null) {
					throw innerException;
				}

				throw;
			}
		}
	}

	public static class BetterParallel {
		public static ParallelLoopResult For(int fromInclusive, int toExclusive, Action<int> body) {
			try {
				return Parallel.For(fromInclusive, toExclusive, body);
			} catch (AggregateException ex) {
				var innerException = ex.InnerExceptions.FirstOrDefault();
				if (innerException != null) {
					throw innerException;
				}

				throw;
			}
		}
	}
}
