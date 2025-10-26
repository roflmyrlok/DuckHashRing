namespace DuckSharding.Shard;

using Dapper;
using Microsoft.Data.Sqlite;
using Shared.Models;


public class DuckRepository
{
    private readonly string _connectionString;

    public DuckRepository(IConfiguration configuration)
    {
        var dbFileName = configuration["Database:FileName"] ?? "shard.db";
        _connectionString = $"Data Source={dbFileName}";
        InitializeDatabase();
    }

    private void InitializeDatabase()
    {
        using var connection = new SqliteConnection(_connectionString);
        connection.Open();

        var createTableSql = @"
            CREATE TABLE IF NOT EXISTS Ducks (
                Species TEXT NOT NULL,
                BirthDate TEXT NOT NULL,
                Name TEXT NOT NULL,
                Age INTEGER NOT NULL,
                Weight REAL NOT NULL,
                PRIMARY KEY (Species, BirthDate)
            )";

        connection.Execute(createTableSql);
    }

    public async Task<Duck?> GetAsync(string species, DateTime birthDate)
    {
        using var connection = new SqliteConnection(_connectionString);
        
        var sql = @"
            SELECT Species, BirthDate, Name, Age, Weight 
            FROM Ducks 
            WHERE Species = @Species AND BirthDate = @BirthDate";

        var result = await connection.QuerySingleOrDefaultAsync<DuckDto>(
            sql, 
            new { Species = species, BirthDate = birthDate.ToString("O") });

        return result?.ToDuck();
    }

    public async Task<bool> ExistsAsync(string species, DateTime birthDate)
    {
        using var connection = new SqliteConnection(_connectionString);
        
        var sql = @"
            SELECT COUNT(1) 
            FROM Ducks 
            WHERE Species = @Species AND BirthDate = @BirthDate";

        var count = await connection.ExecuteScalarAsync<int>(
            sql, 
            new { Species = species, BirthDate = birthDate.ToString("O") });

        return count > 0;
    }

    public async Task UpsertAsync(Duck duck)
    {
        using var connection = new SqliteConnection(_connectionString);
        
        var sql = @"
            INSERT INTO Ducks (Species, BirthDate, Name, Age, Weight)
            VALUES (@Species, @BirthDate, @Name, @Age, @Weight)
            ON CONFLICT(Species, BirthDate) 
            DO UPDATE SET 
                Name = @Name,
                Age = @Age,
                Weight = @Weight";

        await connection.ExecuteAsync(sql, new
        {
            Species = duck.Species,
            BirthDate = duck.BirthDate.ToString("O"),
            Name = duck.Name,
            Age = duck.Age,
            Weight = duck.Weight
        });
    }

    public async Task<bool> DeleteAsync(string species, DateTime birthDate)
    {
        using var connection = new SqliteConnection(_connectionString);
        
        var sql = @"
            DELETE FROM Ducks 
            WHERE Species = @Species AND BirthDate = @BirthDate";

        var rowsAffected = await connection.ExecuteAsync(
            sql, 
            new { Species = species, BirthDate = birthDate.ToString("O") });

        return rowsAffected > 0;
    }

    public async Task<List<Duck>> QueryBySpeciesAsync(string species)
    {
        using var connection = new SqliteConnection(_connectionString);
        
        var sql = @"
            SELECT Species, BirthDate, Name, Age, Weight 
            FROM Ducks 
            WHERE Species = @Species
            ORDER BY BirthDate";

        var results = await connection.QueryAsync<DuckDto>(sql, new { Species = species });

        return results.Select(dto => dto.ToDuck()).ToList();
    }

    private class DuckDto
    {
        public string Species { get; set; } = string.Empty;
        public string BirthDate { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
        public double Weight { get; set; }

        public Duck ToDuck()
        {
            return new Duck
            {
                Species = Species,
                BirthDate = DateTime.Parse(BirthDate),
                Name = Name,
                Age = Age,
                Weight = Weight
            };
        }
    }
}