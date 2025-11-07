namespace DuckSharding.Shared.Hasing;

public class SimpleHash : IHashFunction
{
	public uint ComputeHash(string key)
	{
		return unchecked((uint)key.GetHashCode());
	}
}