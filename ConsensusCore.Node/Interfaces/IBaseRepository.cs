namespace ConsensusCore.Node.BaseClasses
{
    public interface IBaseRepository<TCommand>
        where TCommand : BaseCommand
    {
        void SaveNodeData();
    }
}