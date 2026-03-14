using System.Linq.Expressions;
using Npgsql;
using Pgvector;
using RepoDb.DbHelpers;
using RepoDb.Resolvers;

namespace RepoDb;

public static partial class PostgreSqlVectorsGlobalConfiguration
{

    public static GlobalConfiguration UsePostgreSqlVectors(this GlobalConfiguration globalConfiguration)
    {
#pragma warning disable CS0618 // Type or member is obsolete
        NpgsqlConnection.GlobalTypeMapper.UseVector();
#pragma warning restore CS0618 // Type or member is obsolete


        PostgreSqlDbTypeNameToClientTypeResolver.VectorType = typeof(Vector);
        PostgreSqlDbTypeNameToClientTypeResolver.SparseVectorType = typeof(SparseVector);
#if NET
        PostgreSqlDbTypeNameToClientTypeResolver.HalfVectorType = typeof(HalfVector);
#endif


        PostgreSqlDbHelper.MaybeUpdateNpgsqlParameterCallback = (ref value, p) =>
        {
            if (value is ReadOnlyMemory<float> vf)
            {
                value = new Vector(vf);
                p.DataTypeName = "vector";
                return true;
            }
            else if (value is Vector)
            {
                p.DataTypeName = "vector";
                return true;
            }
#if NET
            else if (value is ReadOnlyMemory<Half> hf)
            {
                value = new HalfVector(hf);
                p.DataTypeName = "halfvec";
                return true;
            }
            else if (value is HalfVector)
            {
                p.DataTypeName = "halfvec";
                return true;
            }
#endif

            return false;
        };

        PostgreSqlDbHelper.ProviderSpecificTypeTransforms.TryAdd((typeof(ReadOnlyMemory<float>), typeof(Vector)),
            (fromExpr) => Expression.New(typeof(Vector).GetConstructor([typeof(ReadOnlyMemory<float>)])!, [fromExpr])
        );
        PostgreSqlDbHelper.ProviderSpecificTypeTransforms.TryAdd((typeof(Vector), typeof(ReadOnlyMemory<float>)),
            (fromExpr) => Expression.Property(fromExpr, nameof(Vector.Memory))
        );
#if NET
        PostgreSqlDbHelper.ProviderSpecificTypeTransforms.TryAdd((typeof(ReadOnlyMemory<Half>), typeof(HalfVector)),
            (fromExpr) => Expression.New(typeof(HalfVector).GetConstructor([typeof(ReadOnlyMemory<Half>)])!, [fromExpr])
        );
        PostgreSqlDbHelper.ProviderSpecificTypeTransforms.TryAdd((typeof(HalfVector), typeof(ReadOnlyMemory<Half>)),
            (fromExpr) => Expression.Property(fromExpr, nameof(Vector.Memory))
        );
#endif

        //PostgreSqlBootstrap.InitializeInternal();
        return globalConfiguration;
    }

}
