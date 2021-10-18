using Models;

namespace IqFeedDownloaderLib
{
    public interface IOhlcRepoSaver<TOhlc, TTime> where TOhlc : Ohlc<TTime>
    {
        public void Save(TOhlc ohlc);
    }
}