using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Text;
using TemporalTest.Models;
using Microsoft.EntityFrameworkCore;
using System.ComponentModel.DataAnnotations.Schema;

namespace TemporalTest.Models;

/*
 * docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=Password123" -p 1433:1433 --name sql1 --hostname sql1 -d mcr.microsoft.com/mssql/server:2022-latest
 * dotnet ef database drop
 * y
 * dotnet ef migrations remove
 * dotnet ef migrations add initial
 * dotnet ef database update
 * 
 */

public class Person
{
    public Guid Id { get; set; }
    public string Name { get; set; }

    public List<PersonVersion> Versions { get; set; } = new();


    [NotMapped]
    public PersonVersion LastestVersion => Versions.SingleOrDefault();
}

public class PersonVersion
{
    public Guid Id { get; set; }
    public Guid PersonId { get; set; }
    public Person Person { get; set; }

    public List<Address> Addresses { get; set; } = new();

    public bool ValidLoad => Addresses.Count == 1;

    [NotMapped]
    public Address LatestAddress => Addresses.SingleOrDefault();

    public override string ToString() => $"{Id} - ValidLoad={ValidLoad} AddressCount={Addresses.Count()}";
}

public class Address
{
    public Guid Id { get; set; }
    public string Name { get; set; }
    public string City { get; set; }

    public PersonVersion Version { get; set; }
    public Guid VersionId { get; set; }

    public override string ToString() => $"{Id} - {City}";
}


public class TestDbContext : DbContext
{
    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseSqlServer("server=localhost;database=TemporalTest;user=sa;password=Password123;TrustServerCertificate=True");
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<Person>(entity =>
        {
            entity.Property(e => e.Name).IsRequired();
        });
        modelBuilder.Entity<PersonVersion>(entity =>
        {
            entity.HasOne(x => x.Person).WithMany(y => y.Versions).HasForeignKey(x => x.PersonId);
            entity.ToTable(nameof(PersonVersion), t => t.IsTemporal());
        });

        modelBuilder.Entity<Address>(entity =>
        {
            entity.Property(e => e.Name).IsRequired();
            entity.Property(e => e.City).IsRequired();
            entity.HasOne(x => x.Version).WithMany(y => y.Addresses).HasForeignKey(x => x.VersionId);
            entity.ToTable(nameof(Address), t => t.IsTemporal());
        });
    }
}


public class Program
{

    static void Main(string[] args)
    {
        var services = new ServiceCollection();
        services.AddDbContext<TestDbContext>();
        var sp = services.BuildServiceProvider();
        var db = sp.GetRequiredService<TestDbContext>();
        var personId = InsertData(db);
        PrintData(db, personId);
        Console.ReadLine();
    }

    private static Guid InsertData(TestDbContext db)
    {
        // Creates the database if not exists
        db.Database.EnsureCreated();

        // Adds a publisher
        var person = new Person
        {
            Id = Guid.NewGuid(),
            Name = "Mariner Books",
            Versions = new List<PersonVersion>
            {
                new()
                {
                    Id = Guid.NewGuid(),
                }
            }
        };
        db.Add(person);
        db.Add(person.LastestVersion);

        var add = new Address
        {
            Id = Guid.NewGuid(),
            Name = "Home",
            City = "Wrong City",
        };
        person.LastestVersion.Addresses.Add(add);
        db.Add(add);


        // Saves changes
        db.SaveChanges();

        Console.WriteLine("waiting a bit so that second address has different time stamp");
        Thread.Sleep(200);

        var add2 = new Address
        {
            Id = Guid.NewGuid(),
            Name = "Home",
            City = "Correct City",
        };
        person.LastestVersion.Addresses.Add(add2);
        db.Add(add2);
        db.SaveChanges();

        return person.Id;
    }


    private static void PrintData(TestDbContext db, Guid personId)
    {
        var currentDate = DateTime.UtcNow;

        //cannot query temporal as of on person table as it's not temporal
        //var person = db.Set<Person>()
        //    .TemporalAsOf(currentDate)
        //    .Include(x => x.Versions)
        //    .ThenInclude(x => x.Addresses)
        //    .Where(x => x.Id == personId)
        //    .FirstOrDefault()
        //    ;
        //Console.WriteLine(person.Id);
        //Console.WriteLine(person..Id);
        //Console.WriteLine(person..LatestAddress.City);

        //can do temporal on address directly
        var address = db.Set<Address>()
            .TemporalAsOf(currentDate)
            .Where(x => x.Version.PersonId == personId)
            .First()
            ;
        Console.WriteLine(address.Id);
        Console.WriteLine(address.City);

        //can do temporal on person version... however-
        var version = db.Set<PersonVersion>()
            .TemporalAsOf(currentDate)
            .Include(x => x.Addresses)
            .Where(x => x.PersonId == personId)
            .First()
            ;

        Console.WriteLine();
        Console.WriteLine();

        Console.WriteLine($"Version: {version}");
        Console.WriteLine($"Version Addresses");

        //- however, addresses are not automatically temporally filtered
        foreach (var a in version.Addresses)
        {
            Console.WriteLine(a);
        }


        //so instead you have to manually join on all tables that are temporal
        var join = db.Set<PersonVersion>()
            .TemporalAsOf(currentDate)
            .Where(x => x.PersonId == personId)
            .Join(
                db.Set<Address>().TemporalAsOf(currentDate),
                p => p.Id, a => a.VersionId, (v, a) => new
                {
                    Version = v,
                    Address = a
                })
            .First()
            ;

        //manually fixup the version
        var versionJoined = join.Version;
        versionJoined.Addresses = new() { join.Address };

        Console.WriteLine();
        Console.WriteLine();
        Console.WriteLine($"Joined Version: {versionJoined}");
        Console.WriteLine($"Joined Version Addresses");
        foreach (var a in versionJoined.Addresses)
        {
            Console.WriteLine(a);
        }
    }
}