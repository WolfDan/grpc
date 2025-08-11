defmodule Grpc.Client.Adapters.Finch.CustomStream do
  alias GRPC.Client.Adapters.Finch.StreamState

  def start() do
    # This is blocking the main process, the reason is that it answer to a process
    # replying to the main process which is wrong

    IO.inspect(self(), label: :calling)

    with {:ok, pid} <- StreamState.start_link(nil) do
      stream =
        Stream.unfold(pid, fn pid ->
          IO.inspect(self(), label: :unfold)

          case StreamState.next_item(pid) do
            :close ->
              nil

            item ->
              IO.inspect(item, label: :item_response)
              {item, pid}
          end
        end)

      # stream =
      #   Stream.resource(
      #     fn -> pid end,
      #     fn pid ->
      #       case StreamState.next_item(pid) do
      #         :close ->
      #           IO.inspect(:halt, label: :item_response)
      #           {:halt, pid}

      #         item ->
      #           IO.inspect(item, label: :item_response)
      #           {[item], pid}
      #       end
      #     end,
      #     &GenServer.stop/1
      #   )

      {:ok, {stream, pid}}
    end
  end

  def add_item(pid, item) do
    StreamState.add_item(pid, item)
  end

  def close(pid) do
    StreamState.close(pid)
  end
end
