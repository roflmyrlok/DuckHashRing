namespace DuckSharding.Shared.Models;

public class Duck
{
	public string Species { get; set; } = string.Empty;
	public DateTime BirthDate { get; set; }
	public string Name { get; set; } = string.Empty;
	public int Age { get; set; }
	public double Weight { get; set; }

	public Duck()
	{
	}

	public Duck(string species, DateTime birthDate, string name, int age, double weight)
	{
		Species = species;
		BirthDate = birthDate;
		Name = name;
		Age = age;
		Weight = weight;
	}

	public string GetCompositeKey()
	{
		return $"{Species}#{BirthDate:O}";
	}
	
	public static string GetCompositeKey(string species, DateTime birthDate)
	{
		return $"{species}#{birthDate:O}";
	}
	
	public bool HasSameKey(Duck other)
	{
		return Species == other.Species && BirthDate == other.BirthDate;
	}

	public bool HasSameKey(string species, DateTime birthDate)
	{
		return Species == species && BirthDate == birthDate;
	}
}