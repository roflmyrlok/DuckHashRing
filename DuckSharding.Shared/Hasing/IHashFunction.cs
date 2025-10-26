namespace DuckSharding.Shared.Hasing;

public interface IHashFunction
{
	uint ComputeHash(string key);
}