/**
 * Gherkin Peeler
 *
 * A simple scalper for the London Multi-Asset eXchange (LMAX)
 */

package com.lmax.gherkin;

import java.util.List;

import com.lmax.api.*;
import com.lmax.api.account.*;
import com.lmax.api.account.LoginRequest.*;
import com.lmax.api.order.*;
import com.lmax.api.orderbook.*;
import com.lmax.api.reject.*;
import com.lmax.api.heartbeat.*;
import com.lmax.api.position.*;

public class GherkinPeeler implements LoginCallback, OrderBookEventListener, OrderEventListener, InstructionRejectedEventListener, ExecutionEventListener, StreamFailureListener, SessionDisconnectedListener, HeartbeatEventListener, PositionEventListener, Runnable
{

  /*
   * Constants
   */

  private static enum GherkinState {WARMUP, READY_TO_OPEN, WAIT_FOR_OPEN, READY_TO_CLOSE, WAIT_FOR_CLOSE};
  private static final short warmupMax = 8;  // Number of updates to collect for each currency before activating trading

  private static final long pos_EUR_USD = 0;
  private static final long pos_GBP_USD = 1;
  private static final long pos_EUR_GBP = 2;

  private static final long id_EUR_USD = 4001;
  private static final long id_GBP_USD = 4002;
  private static final long id_EUR_GBP = 4003;

  private static final long scaleFactor = FixedPointNumber.ONE.longValue();
  private static final long leverageFactor = 8;  // Should change according to wallet size
  private static final long instrumentId[] = {id_EUR_USD, id_GBP_USD, id_EUR_GBP};
  private static final String instrumentDesc[] = {"EUR_USD", "GBP_USD", "EUR_GBP"};
  private static final short bookLevel = 2;  // 0=top, 4=max_depth_available

  /*
   * Variables
   */

  private Session session;
  private GherkinState state;
  private static short warmupCount[] = new short[3];

  private static FixedPointNumber[] lastBid = new FixedPointNumber[3];
  private static FixedPointNumber[] lastAsk = new FixedPointNumber[3];

  private static FixedPointNumber[] lastBidQuantity = new FixedPointNumber[3];
  private static FixedPointNumber[] lastAskQuantity = new FixedPointNumber[3];

  private static FixedPointNumber[] maxSpread = new FixedPointNumber[3];
  private static FixedPointNumber[] minSpread = new FixedPointNumber[3];

  private static short outstandingPos;
  private static FixedPointNumber outstandingQuantity;
  private static FixedPointNumber closingPriceMin;
  private static FixedPointNumber closingPriceMax;

  /*
   * Constructor
   */

  GherkinPeeler(String url, String username, String password, ProductType productType) {
    LmaxApi lmaxApi = new LmaxApi(url);
    lmaxApi.login(new LoginRequest(username, password, productType), this);
  }

  /*
   * Overridden methods
   */

  @Override
  public void onLoginSuccess(Session session)
  {
    System.out.println(" ### GHERKIN_LOGIN_SUCCESS: " + session.getAccountDetails().getAccountId());

    // Save the session for later use.
    this.session = session;

    // Register event listeners and callbacks and start the session
    GherkinStart();
  }

  @Override
  public void onLoginFailure(FailureResponse failureResponse)
  {
    System.err.println(" ### GHERKIN_LOGIN_FAILED: " + failureResponse);
    GherkinStop();
    System.exit(-1);
  }

  @Override
  public void notify(OrderBookEvent orderBookEvent)
  {

    if (orderBookEvent.getBidPrices().size() == 0 || orderBookEvent.getAskPrices().size() == 0)
    {
      System.err.println(" # GHERKIN_NOTIFY_ORDERBOOK_EMPTY_PRICE_LIST");
      return;
    }
    else if (orderBookEvent.getBidPrices().size() < 5 || orderBookEvent.getAskPrices().size() < 5)
    {
      System.err.println(" # GHERKIN_NOTIFY_ORDERBOOK_POOR_PRICE_LIST");
      return;
    }

    /*
     * Always update prices and quantities
     */

    short updatedPos;
    final long id = orderBookEvent.getInstrumentId();
    if      (id == instrumentId[0]) updatedPos = 0;
    else if (id == instrumentId[1]) updatedPos = 1;
    else if (id == instrumentId[2]) updatedPos = 2;
    else return;

    // Update prices
    lastBid[updatedPos] = orderBookEvent.getBidPrices().get(bookLevel).getPrice();
    lastAsk[updatedPos] = orderBookEvent.getAskPrices().get(bookLevel).getPrice();

    // Update quantities
    lastBidQuantity[updatedPos] = orderBookEvent.getBidPrices().get(bookLevel).getQuantity();
    lastAskQuantity[updatedPos] = orderBookEvent.getAskPrices().get(bookLevel).getQuantity();

    // Also update the maximum and minumum spread to use the average value in order to avoid false triggers
    long currentSpread = lastAsk[updatedPos].longValue() - lastBid[updatedPos].longValue();
    if (currentSpread > maxSpread[updatedPos].longValue())
    {
      maxSpread[updatedPos] = FixedPointNumber.valueOf(currentSpread);
    }
    if (currentSpread < minSpread[updatedPos].longValue())
    {
      minSpread[updatedPos] = FixedPointNumber.valueOf(currentSpread);
    }

    // Print the current values of the currency pairs
    for (short i=0; i<3; i++)
    {
      System.out.print(" * " + instrumentDesc[i] + "=" + lastBid[i] + "/" + lastAsk[i]);
    }
    System.out.println();

    // Print the current values of the pair spreads
    for (short i=0; i<3; i++)
    {
      System.out.print(" ** spread=" + minSpread[i] + "/" + maxSpread[i]);
    }
    System.out.println();

    /*
     * Now check if there are orders to place
     */

    switch (state)
    {
      case WARMUP:
      {
        warmupCount[updatedPos]++;
        if (warmupCount[0] >= warmupMax && warmupCount[1] >= warmupMax && warmupCount[2] >= warmupMax &&
            maxSpread[0] != FixedPointNumber.ZERO && maxSpread[1] != FixedPointNumber.ZERO && maxSpread[2] != FixedPointNumber.ZERO &&
            minSpread[0] != FixedPointNumber.ONE  && minSpread[1] != FixedPointNumber.ONE  && minSpread[2] != FixedPointNumber.ONE)
        {
          System.out.println(" ### GHERKIN_NOTIFY_ORDERBOOK_WARMUP_COMPLETE");
          state = GherkinState.READY_TO_OPEN;
        }
        else
        {
          System.out.println(" ## GHERKIN_NOTIFY_ORDERBOOK_WARMUP_NOT_COMPLETE_YET");
        }
        break;
      }
      case READY_TO_OPEN:
      {
        /*
         * Apply the trading idea
         */

        System.out.println(" # GHERKIN_NOTIFY_ORDERBOOK_READY_TO_OPEN");
        for (short i=0; i<3; i++)
        {
          if (i != updatedPos)
          {
            FixedPointNumber derivedBidPrice = getDerivedPrice(i, lastBid);
            FixedPointNumber derivedAskPrice = getDerivedPrice(i, lastAsk);
            long avgSpread = (minSpread[i].longValue() + maxSpread[i].longValue())/2;
            this.outstandingQuantity = FixedPointNumber.ZERO;

            /*
             * Are we going up?
             */

            if (derivedBidPrice.longValue() > lastAsk[i].longValue() + avgSpread)
            {
              this.outstandingPos = i;
              this.outstandingQuantity = getContractQuantity(lastAskQuantity[i], derivedBidPrice.longValue() - lastAsk[i].longValue(), avgSpread);  // Positive
              System.out.println(" ### OPEN WITH BUY " + outstandingQuantity + " x " + instrumentDesc[i] + " NOW AT " + lastBid[i] + "/" + lastAsk[i]);
            }
            else

            /*
             * Are we going down?
             */

            if (derivedAskPrice.longValue() < lastBid[i].longValue() - avgSpread)
            {
              this.outstandingPos = i;
              this.outstandingQuantity = getContractQuantity(lastBidQuantity[i], derivedAskPrice.longValue() - lastBid[i].longValue(), avgSpread);  // Negative
              System.out.println(" ### OPEN WITH SELL " + outstandingQuantity + " x " + instrumentDesc[i] + " NOW AT " + lastBid[i] + "/" + lastAsk[i]);
            }

            /*
             * Place order to open position
             */

            if (outstandingQuantity != FixedPointNumber.ZERO)
            {
              MarketOrderSpecification order = new MarketOrderSpecification(instrumentId[outstandingPos], outstandingQuantity, TimeInForce.FILL_OR_KILL);
              System.out.println(" ## GHERKIN_PLACE_OPEN_ORDER");
              session.placeMarketOrder(order, new GherkinOrderCallback()
              {
                @Override
                public void onSuccess(long instructionId)
                {
                  System.out.println(" ## GHERKIN_PLACE_OPEN_ORDER_SUCCESS");
                  state = GherkinState.WAIT_FOR_OPEN;
                }
              });
              break;  // Without this break there could be two orders in the cycle, the data of the second one overwriting the first one
            }
          }
        }  // for i
        break;
      }
      case WAIT_FOR_OPEN:
      {
        System.out.println(" ## GHERKIN_NOTIFY_ORDERBOOK_WAIT_FOR_OPEN_DO_NOTHING");
        break;
      }
      case READY_TO_CLOSE:
      {
        System.out.println(" # GHERKIN_NOTIFY_ORDERBOOK_READY_TO_CLOSE");
        boolean closeOrder = false;
        if (updatedPos == outstandingPos)
        {
          if (this.outstandingQuantity.longValue() > 0)
          {
            // Buy to sell, best case
            if (lastBid[outstandingPos].longValue() > closingPriceMax.longValue())
            {
              closeOrder = true;
              System.out.println(" ### CLOSE WITH SELL " + outstandingQuantity.negate() + " x " + instrumentDesc[outstandingPos] + " AT " + lastBid[outstandingPos] + " (BEST)");
            }
            else
            // Buy to sell, worst case
            if (lastBid[outstandingPos].longValue() < closingPriceMin.longValue())
            {
              closeOrder = true;
              System.out.println(" ### CLOSE WITH SELL " + outstandingQuantity.negate() + " x " + instrumentDesc[outstandingPos] + " AT " + lastBid[outstandingPos] + " (WORST)");
            }
          }
          else if (this.outstandingQuantity.longValue() < 0)
          {
            // Sell to buy, best case
            if (lastAsk[outstandingPos].longValue() < closingPriceMin.longValue())
            {
              closeOrder = true;
              System.out.println(" ### CLOSE WITH BUY " + outstandingQuantity.negate() + " x " + instrumentDesc[outstandingPos] + " AT " + lastAsk[outstandingPos] + " (BEST)");
            }
            else
            // Sell to buy, worst case
            if (lastAsk[outstandingPos].longValue() > closingPriceMax.longValue())
            {
              closeOrder = true;
              System.out.println(" ### CLOSE WITH BUY " + outstandingQuantity.negate() + " x " + instrumentDesc[outstandingPos] + " AT " + lastAsk[outstandingPos] + " (WORST)");
            }
          }
          else
          {
            System.err.println(" # GHERKIN_MONITOR_PRICES_ZERO_QUANTITY");
            break;
          }
          if (closeOrder)
          {
            // Place closing order
            MarketOrderSpecification order = new MarketOrderSpecification(instrumentId[outstandingPos], outstandingQuantity.negate(), TimeInForce.FILL_OR_KILL);
            System.out.println(" ## GHERKIN_PLACE_CLOSE_ORDER: " + order);
            session.placeMarketOrder(order, new GherkinOrderCallback()
            {
              @Override
              public void onSuccess(long instructionId)
              {
                System.out.println(" ## GHERKIN_PLACE_CLOSE_ORDER_SUCCESS: " + instructionId);
                state = GherkinState.WAIT_FOR_CLOSE;
              }
            });
          }
        }
        break;
      }
      case WAIT_FOR_CLOSE:
      {
        System.out.println(" # GHERKIN_NOTIFY_ORDERBOOK_WAIT_FOR_CLOSE_DO_NOTHING");
        break;
      }
      default:
      {
        System.err.println(" ### GHERKIN_NOTIFY_ORDERBOOK_UNKNOWN_STATE: " + state);
      }
    }  // switch (state)
  }

  @Override
  public void notify(InstructionRejectedEvent instructionRejected)
  {
    final long instructionId = instructionRejected.getInstructionId();
    System.err.println(" ### GHERKIN_NOTIFY_INSTRUCTION_REJECTED: " + instructionId);
  }

  /*
   * The order event will always arrive before the associated execution events for that order.
   */
  @Override
  public void notify(Order order)
  {
    System.out.println(" ## GHERKIN_NOTIFY_ORDER");
  }

  @Override
  public void notify(Execution execution)
  {
    System.out.println(" ### GHERKIN_NOTIFY_EXECUTION: " + execution);
    switch (state)
    {
      case WAIT_FOR_OPEN:
      {
        this.outstandingQuantity = execution.getOrder().getFilledQuantity();
        long doubleSpread = minSpread[this.outstandingPos].longValue() + maxSpread[outstandingPos].longValue();
        this.closingPriceMax = FixedPointNumber.valueOf(execution.getPrice().longValue() + doubleSpread);
        this.closingPriceMin = FixedPointNumber.valueOf(execution.getPrice().longValue() - doubleSpread);

        System.out.println(" ### NOW READY TO CLOSE BELOW " + closingPriceMin + " OR ABOVE " + closingPriceMax);
        state = GherkinState.READY_TO_CLOSE;
        System.out.println(" ## GHERKIN_NOTIFY_EXECUTION_STATE_READY_TO_CLOSE");
        break;
      }
      case WAIT_FOR_CLOSE:
      {
        this.outstandingQuantity = FixedPointNumber.valueOf(this.outstandingQuantity.longValue() + execution.getOrder().getFilledQuantity().longValue());
        if (this.outstandingQuantity.longValue() == 0)
        {
          System.out.println(" ### GHERKIN_NOTIFY_EXECUTION_STATE_READY_TO_OPEN_AGAIN");
          state = GherkinState.READY_TO_OPEN;
        }
        else
        {
          System.out.println(" ### GHERKIN_NOTIFY_EXECUTION_MODIFIED_OUTSTANDING_QUANTITY: " + this.outstandingQuantity);
        }
        break;
      }
      default:
      {
        System.err.println(" ### GHERKIN_NOTIFY_EXECUTION_STATE_UNEXPECTED");
      }
    }
  }

  @Override
  public void notifyStreamFailure(Exception e)
  {
    System.err.println(" ### GHERKIN_STREAM_EXCEPTION");
    GherkinStop();
    GherkinStart();
  }

  @Override
  public void notifySessionDisconnected()
  {
    System.err.println(" ### SESSION_DISCONNECTED_NO_HEARTBEAT");
    GherkinStop();
    GherkinStart();
  }

  @Override
  public void notify(long accountId, String token)
  {
    System.out.printf(" ## GHERKIN_RECEIVED_HEARTBEAT: %d, %s%n", accountId, token);
  }

  @Override
  public void notify(final PositionEvent position)
  {
    System.out.println(" ### GHERKIN_NOTIFY_POSITION_EVENT: " + position);

    if (position.getOpenQuantity().longValue() != 0 && (state == GherkinState.WARMUP || state == GherkinState.READY_TO_OPEN))
    {
      System.out.printf(" ### We shouldn't have open positions, closing %s x %d\n", position.getOpenQuantity(), position.getInstrumentId());
      MarketOrderSpecification order = new MarketOrderSpecification(position.getInstrumentId(), position.getOpenQuantity().negate(), TimeInForce.IMMEDIATE_OR_CANCEL);
      session.placeMarketOrder(order, new GherkinOrderCallback()
      {
        @Override
        public void onSuccess(long instructionId)
        {
          System.out.println(" ### GHERKIN_PLACE_CLOSE_POSITION_SUCCESS: " + instructionId);
        }
      });
    }
  }

  @Override
  public void run()
  {
    try
    {
      while (!Thread.currentThread().isInterrupted())
      {
        Thread.sleep(300000L);  // 5 min => msec
        session.requestHeartbeat(new HeartbeatRequest("token"), new HeartbeatCallback()
        {
          @Override
          public void onSuccess(String token)
          {
            System.out.println(" ## GHERKIN_REQUESTED_HEARTBEAT: " + token);
          }
            
          @Override
          public void onFailure(FailureResponse failureResponse)
          {
            throw new RuntimeException(" ### GHERKIN_REQUEST_HEARTBEAT_FAILED");
          }
        });
      }
    }
    catch (Exception e)
    {
      System.err.println(" ### GHERKIN_RUNNABLE_INTERRUPTED_EXCEPTION_STACK");
      GherkinStop();
      GherkinStart();
    }
  }

  /*
   * New methods
   */

  private void GherkinStart()
  {
    // Reset variables to their initial values
    state = GherkinState.WARMUP;
    for (short i=0; i<3; i++)
    {
      warmupCount[i] = 0;
      lastBid[i] = FixedPointNumber.ZERO;
      lastAsk[i] = FixedPointNumber.ZERO;
      lastBidQuantity[i] = FixedPointNumber.ZERO;
      lastAskQuantity[i] = FixedPointNumber.ZERO;
      maxSpread[i] = FixedPointNumber.ZERO;
      minSpread[i] = FixedPointNumber.ONE;
    }

    // Add listeners and callbacks for all the events I am interested in
    session.registerOrderBookEventListener(this);
    session.registerInstructionRejectedEventListener(this);
    session.registerOrderEventListener(this);
    session.registerExecutionEventListener(this);
    session.registerStreamFailureListener(this);
    session.registerSessionDisconnectedListener(this);
    session.registerPositionEventListener(this);
    session.subscribe(new OrderSubscriptionRequest(), new GherkinSubscriptionCallback());
    for(int i = 0; i < 3; i++)
    {
      session.subscribe(new OrderBookSubscriptionRequest(instrumentId[i]), new GherkinSubscriptionCallback());
    }
    session.registerHeartbeatListener(this);
    session.subscribe(new HeartbeatSubscriptionRequest(), new Callback()
    {
      @Override
      public void onSuccess()
      {
        System.out.println(" ## GHERKIN_SUBSCRIBE_HEARTBEAT_SUCCESS");
      }

      @Override
      public void onFailure(final FailureResponse failureResponse)
      {
        throw new RuntimeException(" ### GHERKIN_SUBSCRIBE_HEARTBEAT_FAILED");
      }
    });
    new Thread(this).start();

    // Start the event processing loop, this method will block until the session is stopped.
    session.start();
  }

  private void GherkinStop()
  {
    session.stop();
  }

  private static FixedPointNumber getDerivedPrice(short instrumentPosToEvaluate, FixedPointNumber[] currentPrices)
  {
    switch (instrumentPosToEvaluate)
    {
      case 0: return FixedPointNumber.valueOf(currentPrices[1].longValue() * currentPrices[2].longValue() / scaleFactor);
      case 1: return FixedPointNumber.valueOf(currentPrices[0].longValue() * scaleFactor / currentPrices[2].longValue());
      case 2: return FixedPointNumber.valueOf(currentPrices[0].longValue() * scaleFactor / currentPrices[1].longValue());
      default: System.out.println(" ### Function getDerivedPrice called with unknown instrumentPosToEvaluate = " + instrumentPosToEvaluate);
    }
    return FixedPointNumber.valueOf("0");
  }

  private static FixedPointNumber getContractQuantity(FixedPointNumber availableQuantity, long leapOpportunity, long avgSpread)
  {
    long tenthsOfContract = 10 * leverageFactor * (leapOpportunity - avgSpread) / avgSpread;
    FixedPointNumber suggestedQuantity = FixedPointNumber.valueOf(scaleFactor * tenthsOfContract / 10);
    if (availableQuantity.longValue() <= suggestedQuantity.longValue())
      return availableQuantity;
    return suggestedQuantity;
  }

  /*
   * Nested classes containing callbacks
   */

  private abstract class GherkinOrderCallback implements OrderCallback
  {
    @Override
    public void onFailure(FailureResponse failureResponse)
    {
      System.err.println(" ### GHERKIN_CALLBACK_FAILURE: " + failureResponse);
      if (!failureResponse.isSystemFailure())
      {
        System.err.printf(" ### GHERKIN_DATA_ERROR: Message: %s, Description: %s", failureResponse.getMessage(), failureResponse.getDescription());
      }
      else
      {
        Exception e = failureResponse.getException();
        if (null != e)
        {
          System.err.print(" ### GHERKIN_EXCEPTION_STRACKTRACE: ");
          e.printStackTrace();
        }
        else
        {
          System.err.printf(" ### GHERKIN_SYSTEM_ERROR: Message: %s, Description: %s", failureResponse.getMessage(), failureResponse.getDescription());
        }
      }
      GherkinStop();
      GherkinStart();
    }
  }

  private final class GherkinSubscriptionCallback implements Callback
  {
    @Override
    public void onSuccess()
    {
      System.out.println(" ## GHERKIN_SUBSCRIPTION_CALLBACK_OK");
    }

    @Override
    public void onFailure(FailureResponse failureResponse)
    {
      System.err.println(" ### GHERKIN_SUBSCRIPTION_CALLBACK_FAILED: " + failureResponse);
      GherkinStop();
      System.exit(-1);
    }
  }

  /*
   * Main function
   */

  public static void main(String[] args)
  {
    if (args.length != 4)
    {
      System.err.println("Usage " + GherkinPeeler.class.getName() + " <url> <username> <password> [CFD_DEMO|CFD_LIVE]");
      System.exit(-1);
    }
    
    String url = args[0];
    String username = args[1];
    String password = args[2];
    ProductType productType = ProductType.valueOf(args[3].toUpperCase());

    GherkinPeeler gherkinPeeler = new GherkinPeeler(url, username, password, productType);
  }

}


