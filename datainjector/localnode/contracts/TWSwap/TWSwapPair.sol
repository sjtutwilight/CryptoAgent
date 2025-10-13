//SPDX-License-Identifier: MIT
pragma solidity ^0.8.21;
import "./interfaces/ITWSwapFactory.sol";
import "@openzeppelin/contracts/token/ERC20/extensions/ERC20Permit.sol";
import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {Ownable} from "@openzeppelin/contracts/access/Ownable.sol";
import "./libraries/UQ112x112.sol";
import "./libraries/Math.sol";
import "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";

contract TWSwapPair is ERC20Permit {
    using UQ112x112 for uint224;
    using SafeERC20 for IERC20;
    event Mint(address indexed sender, uint amount0, uint amount1);
    event Burn(
        address indexed sender,
        uint amount0,
        uint amount1,
        address indexed to
    );
    event Swap(
        address indexed sender,
        uint amount0In,
        uint amount1In,
        uint amount0Out,
        uint amount1Out,
        address indexed to
    );
    event Sync(uint112 reserve0, uint112 reserve1);
    uint256 public constant MINIMUM_LIQUIDITY = 10 ** 3;
    uint8 private unlocked = 1;
    address public factory;
    address public token0;
    address public token1;
    uint112 public reserve0;
    uint112 public reserve1;
    uint32 blockTimestampLast;
    uint256 price0CumulativeLast;
    uint256 price1CumulativeLast;
    uint256 klast;
    modifier lock() {
        require(unlocked == 1, "locked");
        unlocked = 0;
        _;
        unlocked = 1;
    }
    constructor() ERC20("LPToken", "LPToken") ERC20Permit("LPToken") {
        factory = msg.sender;
    }
    function initialize(address _token0, address _token1) external {
        require(msg.sender == factory, "initialize must be called by factory");
        token0 = _token0;
        token1 = _token1;
    }
    function _update(
        uint256 balance0,
        uint256 balance1,
        uint112 _reserve0,
        uint112 _reserve1
    ) private {
        require(
            balance0 <= type(uint256).max && balance1 <= type(uint256).max,
            "overflow"
        );
        uint32 currentTimestamp = uint32(block.timestamp);
        uint32 timeElapsed = currentTimestamp - blockTimestampLast;
        if (timeElapsed > 0 && _reserve0 != 0 && _reserve1 != 0) {
            price0CumulativeLast +=
                uint256(UQ112x112.encode(_reserve1).uqdiv(_reserve0)) *
                timeElapsed;
            price1CumulativeLast +=
                uint256(UQ112x112.encode(_reserve0).uqdiv(_reserve1)) *
                timeElapsed;
        }
        reserve0 = uint112(balance0);
        reserve1 = uint112(balance1);
        blockTimestampLast = currentTimestamp;
        emit Sync(reserve0, reserve1);
    }
    function mint(address to) external lock returns (uint256 liquidity) {
        (uint112 _reserve0, uint112 _reserve1, ) = getReserves();

        uint256 balance0 = IERC20(token0).balanceOf(address(this));
        uint256 balance1 = IERC20(token1).balanceOf(address(this));

        uint256 amount0 = balance0 - _reserve0;
        uint256 amount1 = balance1 - _reserve1;

        uint256 _totalSupply = totalSupply();
        if (_totalSupply == 0) {
            liquidity = Math.sqrt(amount0 * amount1) - (MINIMUM_LIQUIDITY);
            _mint(
                address(0x000000000000000000000000000000000000dEaD),
                MINIMUM_LIQUIDITY
            );
        } else {
            liquidity = Math.min(
                (amount0 * _totalSupply) / _reserve0,
                (amount1 * _totalSupply) / _reserve1
            );
        }

        require(liquidity > 0, "insufficient input");
        _mint(to, liquidity);

        _update(balance0, balance1, _reserve0, _reserve1);

        emit Mint(msg.sender, amount0, amount1);
    }
    function burn(
        address to
    ) external lock returns (uint256 amount0, uint256 amount1) {
        (uint112 _reserve0, uint112 _reserve1, ) = getReserves();
        address _token0 = token0;
        address _token1 = token1;
        uint256 balance0 = IERC20(_token0).balanceOf(address(this));
        uint256 balance1 = IERC20(_token1).balanceOf(address(this));
        uint256 liquidity = balanceOf(address(this));
        uint256 totalSupply = totalSupply();
        amount0 = (liquidity * balance0) / totalSupply;
        amount1 = (liquidity * balance1) / totalSupply;
        require(amount0 > 0 && amount1 > 0, "insufficient liquidity burned");
        _burn(address(this), liquidity);
        IERC20(token0).safeTransfer(to, amount0);
        IERC20(token1).safeTransfer(to, amount1);
        balance0 = IERC20(_token0).balanceOf(address(this));
        balance1 = IERC20(_token1).balanceOf(address(this));
        _update(balance0, balance1, _reserve0, _reserve1);
        emit Burn(msg.sender, amount0, amount1, to);
    }
    function swap(
        uint256 amount0Out,
        uint256 amount1Out,
        address to
    ) external lock {
        require(amount0Out > 0 || amount1Out > 0, "insufficient amount");
        (uint112 _reserve0, uint112 _reserve1, ) = getReserves();
        require(
            amount0Out < _reserve0 && amount1Out < _reserve1,
            "no enough liquidity"
        );
        uint256 balance0;
        uint256 balance1;
        {
            address _token0 = token0;
            address _token1 = token1;
            require(to != _token0 && to != _token1, "invalid to");

            if (amount0Out > 0) {
                IERC20(_token0).safeTransfer(to, amount0Out);
            } else {
                IERC20(_token1).safeTransfer(to, amount1Out);
            }

            balance0 = IERC20(_token0).balanceOf(address(this));
            balance1 = IERC20(_token1).balanceOf(address(this));
        }

        uint256 amount0In = balance0 > uint256(_reserve0)
            ? balance0 - uint256(_reserve0)
            : 0;
        uint256 amount1In = balance1 > uint256(_reserve1)
            ? balance1 - uint256(_reserve1)
            : 0;

        {
            uint256 balance0Ajusted = balance0 * 1000 - amount0In * 3;
            uint256 balance1Ajusted = balance1 * 1000 - amount1In * 3;

            require(
                balance0Ajusted * balance1Ajusted >=
                    uint256(_reserve0) * uint256(_reserve1) * 1000 * 1000,
                "not enough amount in"
            );
        }

        _update(balance0, balance1, _reserve0, _reserve1);
        emit Swap(msg.sender, amount0In, amount1In, amount0Out, amount1Out, to);
    }
    function getReserves()
        public
        view
        returns (
            uint112 _reserve0,
            uint112 _reserve1,
            uint32 _blockTimestampLast
        )
    {
        _reserve0 = reserve0;
        _reserve1 = reserve1;
        _blockTimestampLast = blockTimestampLast;
    }
}
